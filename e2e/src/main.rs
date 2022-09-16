use std::{
    io::{ErrorKind, Read, Write},
    net::{SocketAddr, TcpListener, TcpStream},
    os::unix::prelude::AsRawFd,
    str::from_utf8,
    thread::{self, JoinHandle},
};

use futures::channel::mpsc;
use mio::net::UnixStream;

use sozu_command_lib as sozu_command;
use sozu_lib as sozu;

use sozu::server::Server;
use sozu_command::{
    channel::Channel,
    config::{Config, FileConfig},
    proxy::{
        ActivateListener, Backend, Cluster, HttpFrontend, HttpListener, ListenerType,
        LoadBalancingAlgorithms, LoadBalancingParams, PathRule, ProxyRequest, ProxyRequestOrder,
        ProxyResponse, Route, RulePosition,
    },
    scm_socket::{Listeners, ScmSocket},
    state::ConfigState,
};

trait Aggregator {
    fn receive(&mut self);
}

#[derive(Debug, Clone)]
struct SimpleAggregator {
    received: usize,
    sent: usize,
}
impl Aggregator for SimpleAggregator {
    fn receive(&mut self) {
        self.received += 1;
    }
}

struct CommandID {
    id: usize,
    prefix: String,
    last: String,
}
impl CommandID {
    fn new() -> Self {
        Self {
            id: 0,
            prefix: "ID_".to_owned(),
            last: "NONE".to_owned(),
        }
    }
    fn next(&mut self) -> String {
        let id = format!("{}{}", self.prefix, self.id);
        self.last = id.to_owned();
        self.id += 1;
        id
    }
}

/// Handle to a detached thread where a TcpListener runs
struct MockBackend<T> {
    stop_tx: mpsc::Sender<()>,
    aggregator_rx: mpsc::Receiver<T>,
}

impl<T: Aggregator + Send + Sync + 'static> MockBackend<T> {
    fn new(
        address: SocketAddr,
        mut aggregator: T,
        handler: Box<dyn Fn(TcpStream, T) -> T + Send + Sync>,
    ) -> Self {
        let (stop_tx, mut stop_rx) = mpsc::channel::<()>(1);
        let (mut aggregator_tx, aggregator_rx) = mpsc::channel::<T>(1);
        let listener = TcpListener::bind(address).expect("could not bind");
        thread::spawn(move || {
            listener
                .set_nonblocking(true)
                .expect("could not set nonblocking on listener");
            loop {
                let stream = listener.accept();
                match stream {
                    Ok(stream) => {
                        aggregator.receive();
                        aggregator = handler(stream.0, aggregator)
                    }
                    Err(error) => {
                        if error.kind() != ErrorKind::WouldBlock {
                            println!("IO Error: {:?}", error);
                        }
                    }
                }
                match stop_rx.try_next() {
                    Ok(Some(_)) => break,
                    _ => continue,
                }
            }
            aggregator_tx.try_send(aggregator).expect("could not send aggregator");
        });
        Self { stop_tx, aggregator_rx }
    }
    fn stop_and_get_aggregator(&mut self) -> Option<T> {
        self.stop_tx.try_send(()).expect("could not stop backend");
        loop {
            match self.aggregator_rx.try_next() {
                Ok(Some(aggregator)) => return Some(aggregator),
                _ => continue,
            }
        }
    }
}

impl MockBackend<SimpleAggregator> {
    fn http_response<S: Into<String>>(
        content: S,
    ) -> Box<dyn Fn(TcpStream, SimpleAggregator) -> SimpleAggregator + Send + Sync> {
        let content = content.into();
        Box::new(move |mut stream: TcpStream, mut aggregator: SimpleAggregator| {
            // let buf_reader = BufReader::new(&mut stream);
            // let request = buf_reader
            //     .lines()
            //     .map(|result| result.unwrap())
            //     .take_while(|line| !line.is_empty())
            //     .collect::<Vec<_>>();
            let status_line = "HTTP/1.1 200 OK";
            let length = content.len();
            let response = format!("{status_line}\r\nContent-Length: {length}\r\n\r\n{content}");
            stream.write_all(response.as_bytes()).unwrap();
            aggregator.sent += 1;
            aggregator
        })
    }
    fn tcp_response<S: Into<String>>(
        content: String,
    ) -> Box<dyn Fn(TcpStream, SimpleAggregator) -> SimpleAggregator + Send + Sync> {
        let content: String = content.into();
        Box::new(move |mut stream: TcpStream, mut aggregator: SimpleAggregator| {
            stream.write_all(content.as_bytes()).unwrap();
            aggregator.sent += 1;
            aggregator
        })
    }
}

struct Worker {
    scm_socket: ScmSocket,
    command_channel: Channel<ProxyRequest, ProxyResponse>,
    command_id: CommandID,
    job: JoinHandle<()>,
}

impl Worker {
    fn empty_config() -> (Config, Listeners) {
        let config = FileConfig {
            command_socket: None,
            saved_state: None,
            automatic_state_save: None,
            worker_count: None,
            worker_automatic_restart: Some(false),
            handle_process_affinity: None,
            command_buffer_size: None,
            max_command_buffer_size: None,
            max_connections: None,
            min_buffers: None,
            max_buffers: None,
            buffer_size: None,
            log_level: None,
            log_target: None,
            log_access_target: None,
            metrics: None,
            listeners: None,
            clusters: None,
            ctl_command_timeout: None,
            pid_file_path: None,
            tls_provider: None,
            activate_listeners: None,
            front_timeout: None,
            back_timeout: None,
            connect_timeout: None,
            zombie_check_interval: None,
            accept_queue_timeout: None,
        };
        let config = config.into("./config.toml");
        let listeners = Listeners {
            http: Vec::new(),
            tls: Vec::new(),
            tcp: Vec::new(),
        };
        (config, listeners)
    }

    fn start_new_worker(config: Config, listeners: Listeners) -> Self {
        let (scm_main, scm_worker) = UnixStream::pair().expect("could not create unix stream pair");
        let (cmd_main, cmd_worker) =
            Channel::generate(config.command_buffer_size, config.max_command_buffer_size)
                .expect("could not create a channel");

        let scm_main = ScmSocket::new(scm_main.as_raw_fd());
        scm_main
            .send_listeners(&listeners)
            .expect("coulnd not send listeners");

        let job = thread::spawn(move || {
            let mut server = Server::new_from_config(
                cmd_worker,
                ScmSocket::new(scm_worker.as_raw_fd()),
                config,
                ConfigState::new(),
                false,
            );
            server.run();
        });

        Self {
            scm_socket: scm_main,
            command_channel: cmd_main,
            command_id: CommandID::new(),
            job,
        }
    }

    fn send_proxy_request(&mut self, order: ProxyRequestOrder) {
        self.command_channel.write_message(&ProxyRequest {
            id: self.command_id.next(),
            order,
        });
    }
    fn read_message(&mut self) -> Option<ProxyResponse> {
        self.command_channel.read_message()
    }
    fn read_to_last(&mut self) {
        loop {
            let response = self.read_message();
            println!("test received: {:?}", response);
            if response.unwrap().id == self.command_id.last {
                break;
            }
        }
    }
}

fn main() {
    let (config, listeners) = Worker::empty_config();
    let mut worker = Worker::start_new_worker(config, listeners);

    let front_address = "127.0.0.1:1031"
        .parse()
        .expect("could not parse front address");
    let back1_address = "127.0.0.1:1028"
        .parse()
        .expect("could not parse backend1 address");
    let back2_address = "127.0.0.1:1029"
        .parse()
        .expect("could not parse backend2 address");

    let http_listener = HttpListener {
        address: front_address,
        ..Default::default()
    };
    let cluster = Cluster {
        cluster_id: String::from("cluster_1"),
        sticky_session: false,
        https_redirect: false,
        proxy_protocol: None,
        load_balancing: LoadBalancingAlgorithms::default(),
        load_metric: None,
        answer_503: None,
    };
    let front = HttpFrontend {
        route: Route::ClusterId(String::from("cluster_1")),
        address: front_address,
        hostname: String::from("localhost"),
        path: PathRule::Prefix(String::from("/")),
        method: None,
        position: RulePosition::Tree,
        tags: None,
    };
    let backend1 = Backend {
        cluster_id: String::from("cluster_1"),
        backend_id: String::from("cluster_1-0"),
        address: back1_address,
        load_balancing_parameters: Some(LoadBalancingParams::default()),
        sticky_id: None,
        backup: None,
    };
    let backend2 = Backend {
        cluster_id: String::from("cluster_1"),
        backend_id: String::from("cluster_1-1"),
        address: back2_address,
        load_balancing_parameters: Some(LoadBalancingParams::default()),
        sticky_id: None,
        backup: None,
    };

    worker.send_proxy_request(ProxyRequestOrder::AddHttpListener(http_listener));
    worker.send_proxy_request(ProxyRequestOrder::ActivateListener(ActivateListener {
        address: front_address,
        proxy: ListenerType::HTTP,
        from_scm: false,
    }));
    worker.send_proxy_request(ProxyRequestOrder::AddCluster(cluster));
    worker.send_proxy_request(ProxyRequestOrder::AddHttpFrontend(front));
    worker.send_proxy_request(ProxyRequestOrder::AddBackend(backend1));
    worker.send_proxy_request(ProxyRequestOrder::AddBackend(backend2));
    worker.send_proxy_request(ProxyRequestOrder::Status);

    worker.read_to_last();

    let aggregator = SimpleAggregator {
        received: 0,
        sent: 0,
    };

    let mut backend1 = MockBackend::new(
        back1_address,
        aggregator.clone(),
        MockBackend::http_response("pong1"),
    );
    let mut backend2 = MockBackend::new(
        back2_address,
        aggregator,
        MockBackend::http_response("pong2"),
    );

    let mut client = TcpStream::connect(front_address).expect("could not connect to sozu");

    let requests = 300;
    let mut responses = 0;
    for _ in 0..requests {
        let bytes_written = client.write(&b"GET / HTTP/1.1\r\nHost: localhost\r\nConnection: keep-alive\r\n\r\n"[..]);
        println!("http client write: {:?}", bytes_written);
        let mut buffer = [0; 4096];
        let bytes_read = client.read(&mut buffer);
        match bytes_read {
            Ok(bytes_read) if bytes_read > 0 => responses+=1,
            _ => (),
        }
        println!("http client read: {:?}", bytes_read);
        let size = match bytes_read {
            Err(e) => panic!("client request should not fail. Error: {:?}", e),
            Ok(size) => size,
        };
        let answer = from_utf8(&buffer[..size]).expect("could not make string from buffer");
        println!("Response: {}", answer);
    }

    println!("sent: {}, received: {}", requests, responses);
    let aggregator = backend1.stop_and_get_aggregator();
    println!("backend1 aggregator: {:?}", aggregator);
    let aggregator = backend2.stop_and_get_aggregator();
    println!("backend2 aggregator: {:?}", aggregator);

    worker.send_proxy_request(ProxyRequestOrder::HardStop);
    println!("waiting...");
    worker.job.join().expect("could not join");
}
