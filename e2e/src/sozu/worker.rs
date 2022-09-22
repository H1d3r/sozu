use std::{
    net::SocketAddr,
    os::unix::prelude::AsRawFd,
    thread::{self, JoinHandle},
};

use mio::net::UnixStream;

use sozu_command_lib as sozu_command;
use sozu_lib as sozu;

use sozu::server::Server;
use sozu_command::{
    channel::Channel,
    config::{Config, FileConfig},
    proxy::{
        Backend, Cluster, HttpFrontend, HttpListener, LoadBalancingAlgorithms, LoadBalancingParams,
        PathRule, ProxyRequest, ProxyRequestOrder, ProxyResponse, Route, RulePosition,
    },
    scm_socket::{Listeners, ScmSocket},
    state::ConfigState,
};

use crate::sozu::command_id::CommandID;

pub struct Worker {
    pub scm_socket: ScmSocket,
    pub command_channel: Channel<ProxyRequest, ProxyResponse>,
    pub command_id: CommandID,
    pub job: JoinHandle<()>,
}

impl Worker {
    pub fn empty_file_config() -> FileConfig {
        FileConfig {
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
        }
    }
    pub fn empty_listeners() -> Listeners {
        Listeners {
            http: Vec::new(),
            tls: Vec::new(),
            tcp: Vec::new(),
        }
    }
    pub fn empty_config() -> (Config, Listeners) {
        let listeners = Worker::empty_listeners();
        let config = Worker::empty_file_config();
        let config = config.into("./config.toml");
        (config, listeners)
    }

    pub fn create_server(config: Config, listeners: Listeners) -> Server {
        let (scm_main, scm_worker) = UnixStream::pair().expect("could not create unix stream pair");
        let (_, cmd_worker) =
            Channel::generate(config.command_buffer_size, config.max_command_buffer_size)
                .expect("could not create a channel");

        let scm_main = ScmSocket::new(scm_main.as_raw_fd());
        scm_main
            .send_listeners(&listeners)
            .expect("coulnd not send listeners");

        Server::new_from_config(
            cmd_worker,
            ScmSocket::new(scm_worker.as_raw_fd()),
            config,
            ConfigState::new(),
            false,
        )
    }

    pub fn start_new_worker(config: Config, listeners: Listeners) -> Self {
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

    pub fn send_proxy_request(&mut self, order: ProxyRequestOrder) {
        self.command_channel.write_message(&ProxyRequest {
            id: self.command_id.next(),
            order,
        });
    }
    pub fn read_proxy_response(&mut self) -> Option<ProxyResponse> {
        self.command_channel.read_message()
    }
    pub fn read_to_last(&mut self) {
        loop {
            let response = self.read_proxy_response();
            println!("test received: {:?}", response);
            if response.unwrap().id == self.command_id.last {
                break;
            }
        }
    }

    pub fn stop(mut self) {
        self.send_proxy_request(ProxyRequestOrder::HardStop);
        if self.job.is_finished() {
            println!("already finished...");
        } else {
            println!("waiting...");
            match self.job.join() {
                Ok(_) => println!("finished!"),
                Err(error) => println!("could not join: {:#?}", error),
            }
        }
    }

    pub fn default_http_listener(address: SocketAddr) -> HttpListener {
        HttpListener {
            address,
            ..HttpListener::default()
        }
    }
    pub fn default_cluster<S: Into<String>>(cluster_id: S) -> Cluster {
        Cluster {
            cluster_id: cluster_id.into(),
            sticky_session: false,
            https_redirect: false,
            proxy_protocol: None,
            load_balancing: LoadBalancingAlgorithms::default(),
            load_metric: None,
            answer_503: None,
        }
    }
    pub fn default_http_frontend<S: Into<String>>(
        cluster_id: S,
        address: SocketAddr,
    ) -> HttpFrontend {
        HttpFrontend {
            route: Route::ClusterId(cluster_id.into()),
            address,
            hostname: String::from("localhost"),
            path: PathRule::Prefix(String::from("/")),
            method: None,
            position: RulePosition::Tree,
            tags: None,
        }
    }
    pub fn default_backend<S1: Into<String>, S2: Into<String>>(
        cluster_id: S1,
        backend_id: S2,
        address: SocketAddr,
    ) -> Backend {
        Backend {
            cluster_id: cluster_id.into(),
            backend_id: backend_id.into(),
            address,
            load_balancing_parameters: Some(LoadBalancingParams::default()),
            sticky_id: None,
            backup: None,
        }
    }
}
