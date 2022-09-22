use http::{http_request, http_response};
use sozu_command_lib::{
    config::{Config, FileConfig},
    proxy::{ActivateListener, ListenerType, ProxyRequestOrder},
    scm_socket::Listeners,
};
use std::net::SocketAddr;

mod http;
mod mock;
mod sozu;

use sozu::worker::Worker;

use mock::{
    aggregator::SimpleAggregator, async_backend::Backend as AsyncBackend, client::Client,
    sync_backend::Backend as SyncBackend,
};

fn test_setup(
    config: Config,
    listeners: Listeners,
    front_address: SocketAddr,
    nb_backends: usize,
) -> (Worker, Vec<SocketAddr>) {
    let mut worker = Worker::start_new_worker(config, listeners);

    worker.send_proxy_request(ProxyRequestOrder::AddHttpListener(
        Worker::default_http_listener(front_address),
    ));
    worker.send_proxy_request(ProxyRequestOrder::ActivateListener(ActivateListener {
        address: front_address,
        proxy: ListenerType::HTTP,
        from_scm: false,
    }));
    worker.send_proxy_request(ProxyRequestOrder::AddCluster(Worker::default_cluster(
        "cluster_0",
    )));
    worker.send_proxy_request(ProxyRequestOrder::AddHttpFrontend(
        Worker::default_http_frontend("cluster_0", front_address),
    ));

    let mut backends = Vec::new();
    for i in 0..nb_backends {
        let back_address = format!("127.0.0.1:{}", 2002 + i)
            .parse()
            .expect("could not parse front address");
        worker.send_proxy_request(ProxyRequestOrder::AddBackend(Worker::default_backend(
            "cluster_0",
            format!("cluster_0-{}", i),
            back_address,
        )));
        backends.push(back_address);
    }

    worker.read_to_last();
    (worker, backends)
}

fn async_test_setup(
    config: Config,
    listeners: Listeners,
    front_address: SocketAddr,
    nb_backends: usize,
) -> (Worker, Vec<AsyncBackend<SimpleAggregator>>) {
    let (worker, backends) = test_setup(config, listeners, front_address, nb_backends);
    let backends = backends
        .into_iter()
        .enumerate()
        .map(|(i, back_address)| {
            let aggregator = SimpleAggregator {
                received: 0,
                sent: 0,
            };
            AsyncBackend::new(
                format!("BACKEND_{}", i),
                back_address,
                aggregator.to_owned(),
                AsyncBackend::http_handler(format!("pong{}", i)),
            )
        })
        .collect::<Vec<_>>();
    (worker, backends)
}

fn sync_test_setup(
    config: Config,
    listeners: Listeners,
    front_address: SocketAddr,
    nb_backends: usize,
) -> (Worker, Vec<SyncBackend>) {
    let (worker, backends) = test_setup(config, listeners, front_address, nb_backends);
    let backends = backends
        .into_iter()
        .enumerate()
        .map(|(i, back_address)| {
            SyncBackend::new(
                format!("BACKEND_{}", i),
                back_address,
                http_response(format!("pong{}", i)),
            )
        })
        .collect::<Vec<_>>();
    (worker, backends)
}

fn test_async(nb_requests: usize) {
    let front_address = "127.0.0.1:2001"
        .parse()
        .expect("could not parse front address");

    let (config, listeners) = Worker::empty_config();
    let (worker, mut backends) = async_test_setup(config, listeners, front_address, 3);

    let mut clients = (0..10)
        .map(|i| {
            Client::new(
                format!("client{}", i),
                front_address,
                http_request("GET", "/api", format!("ping{}", i)),
            )
        })
        .collect::<Vec<_>>();
    for client in clients.iter_mut() {
        client.connect();
    }
    for _ in 0..nb_requests {
        for client in clients.iter_mut() {
            client.send();
        }
        for client in clients.iter_mut() {
            match client.receive() {
                Some(response) => println!("{}", response),
                _ => {}
            }
        }
    }

    for client in clients {
        println!(
            "{} sent: {}, received: {}",
            client.name, client.sent, client.received
        );
    }
    for backend in backends.iter_mut() {
        let aggregator = backend.stop_and_get_aggregator();
        println!("{} aggregated: {:?}", backend.name, aggregator);
    }

    worker.stop();
}

fn test_sync(nb_requests: usize) {
    let front_address = "127.0.0.1:2001"
        .parse()
        .expect("could not parse front address");

    let (config, listeners) = Worker::empty_config();
    let (worker, mut backends) = sync_test_setup(config, listeners, front_address, 1);
    let mut backend = backends.pop().unwrap();

    backend.connect();

    let mut clients = (0..10)
        .map(|i| {
            Client::new(
                format!("client{}", i),
                front_address,
                http_request("GET", "/api", format!("ping{}", i)),
            )
        })
        .collect::<Vec<_>>();
    for (i, client) in clients.iter_mut().enumerate() {
        client.connect();
        client.send();
        backend.accept(i);
        backend.receive(i);
        backend.send(i);
        client.receive();
    }
    for _ in 0..nb_requests {
        for client in clients.iter_mut() {
            client.send();
        }
        for i in 0..clients.len() {
            backend.receive(i);
            backend.send(i);
        }
        for client in clients.iter_mut() {
            match client.receive() {
                Some(response) => println!("{}", response),
                _ => {}
            }
        }
    }

    for client in clients {
        println!(
            "{} sent: {}, received: {}",
            client.name, client.sent, client.received
        );
    }
    println!(
        "{} sent: {}, received: {}",
        backend.name, backend.sent, backend.received
    );

    worker.stop();
}

fn test_issue_806(nb_requests: usize, zombie: bool) {
    let front_address = "127.0.0.1:2001"
        .parse()
        .expect("could not parse front address");

    let config = FileConfig {
        zombie_check_interval: if zombie { Some(1) } else { None },
        ..Worker::empty_file_config()
    };
    let listeners = Worker::empty_listeners();
    let (worker, mut backends) = async_test_setup(config.into(""), listeners, front_address, 2);
    let mut backend2 = backends.pop().expect("backend2");
    let mut backend1 = backends.pop().expect("backend1");

    let mut aggregator = Some(SimpleAggregator {
        received: 0,
        sent: 0,
    });

    let mut client = Client::new("client", front_address, http_request("GET", "/api", "ping"));
    client.connect();

    for i in 0..nb_requests {
        if client.send().is_none() {
            break;
        }
        match client.receive() {
            Some(response) => println!("{}", response),
            None => break,
        }
        if i == 0 {
            aggregator = backend1.stop_and_get_aggregator();
        }
    }

    println!("sent: {}, received: {}", client.sent, client.received);
    println!("backend1 aggregator: {:?}", aggregator);
    aggregator = backend2.stop_and_get_aggregator();
    println!("backend2 aggregator: {:?}", aggregator);

    worker.stop();
}

fn main() {
    test_sync(100);
    test_async(100);
    // https://github.com/sozu-proxy/sozu/issues/806
    test_issue_806(2, false);
    test_issue_806(2, true);
}
