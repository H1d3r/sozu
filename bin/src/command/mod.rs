use std::{
    collections::{HashMap, HashSet},
    fs,
    os::unix::{
        fs::PermissionsExt,
        io::{AsRawFd, FromRawFd, IntoRawFd},
        net::{UnixListener, UnixStream},
    },
    path::PathBuf,
};

use anyhow::{bail, Context};
use async_dup::Arc;
use async_io::Async;
use futures::{
    channel::{mpsc::*, oneshot},
    {SinkExt, StreamExt},
};
use futures_lite::{future, io::*};
use nix::{
    sys::signal::{kill, Signal},
    unistd::Pid,
};
use serde::{Deserialize, Serialize};
use serde_json;

use sozu_command_lib::{
    command::{
        CommandRequest, CommandRequestOrder, CommandResponse, CommandResponseContent,
        CommandStatus, Event, RunState,
    },
    config::Config,
    proxy::{
        MetricsConfiguration, ProxyRequest, ProxyRequestOrder, ProxyResponse, ProxyResponseContent,
        ProxyResponseStatus,
    },
    scm_socket::{Listeners, ScmSocket},
    state::ConfigState,
};

use crate::{
    get_executable_path,
    upgrade::{SerializedWorker, UpgradeData},
    util,
    worker::start_worker,
};

mod orders;
mod worker;

pub use worker::*;

// The CommandServer receives these CommandMessages, either from within Sōzu,
// or from without, in which case they are ALWAYS of the ClientRequest variant.
enum CommandMessage {
    ClientNew {
        id: String,
        sender: Sender<CommandResponse>, // to send things back to the client
    },
    ClientClose {
        id: String,
    },
    ClientRequest {
        id: String,
        message: CommandRequest,
    },
    WorkerResponse {
        id: u32,
        message: ProxyResponse,
    },
    WorkerClose {
        id: u32,
    },
    Advancement {
        request_identifier: RequestIdentifier,
        response: Response,
    },
    MasterStop,
}

/// identifies a request only within the command server
/// the request part does NOT get sent to a worker
#[derive(PartialEq, Eq, Clone, Debug)]
pub struct RequestIdentifier {
    /// the client who sent the request (ex: "CL-0")
    client: String,
    // the request id (ex: "ID-MAN9QF")
    request: String,
}

impl RequestIdentifier {
    pub fn new<T>(client: T, request: T) -> Self
    where
        T: ToString,
    {
        Self {
            client: client.to_string(),
            request: request.to_string(),
        }
    }
}

#[derive(PartialEq, Eq, Clone, Debug)]
pub enum Response {
    Error(String),
    // Todo: refactor the CLI, see issue #740
    // Processing(String),
    Ok(Success),
}

// Indicates success of either inner Sōzu logic and of handling the ClientRequest,
// in which case Success caries the response data.
#[derive(PartialEq, Eq, Clone, Debug)]
pub enum Success {
    ClientClose(String),               // the client id
    ClientNew(String),                 // the client id
    DumpState(CommandResponseContent), // the cloned state
    HandledClientRequest,
    ListFrontends(CommandResponseContent), // the list of frontends
    ListWorkers(CommandResponseContent),
    LoadState(String, usize, usize), // state path, oks, errors
    Logging(String),                 // new logging level
    Metrics(MetricsConfiguration),   // enable / disable / clear metrics on the proxy
    MasterStop,
    // this should contain CommandResponseData but the logic does not return anything
    // is this logic gone into sozu_command_lib::proxy::Query::Metrics(_) ?
    // Metrics,
    NotifiedClient(String), // client id
    PropagatedWorkerEvent,
    Query(CommandResponseContent),
    ReloadConfiguration(usize, usize), // ok, errors
    SaveState(usize, String),          // amount of written commands, path of the saved state
    SubscribeEvent(String),
    UpgradeMain(i32),         // pid of the new main process
    UpgradeWorker(u32),       // worker id
    WorkerKilled(u32),        // worker id
    WorkerLaunched(u32),      // worker id
    WorkerOrder(Option<u32>), // worker id
    WorkerResponse,
    WorkerRestarted(u32), // worker id
    WorkerStopped(u32),   // worker id
}

// This is how success is logged on Sōzu, and, given the case, manifested to the client
impl std::fmt::Display for Success {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Self::ClientClose(id) => write!(f, "Close client: {}", id),
            Self::ClientNew(id) => write!(f, "New client successfully added: {}", id),
            Self::DumpState(_) => write!(f, "Successfully gathered state from the main process"),
            Self::HandledClientRequest => write!(f, "Successfully handled the client request"),
            Self::ListFrontends(_) => write!(f, "Successfully gathered the list of frontends"),
            Self::ListWorkers(_) => write!(f, "Successfully listed all workers"),
            Self::LoadState(path, ok, error) => write!(
                f,
                "Successfully loaded state from path {}, {} ok messages, {} errors",
                path, ok, error
            ),
            Self::Logging(logging_filter) => write!(
                f,
                "Successfully set the logging level to {}",
                logging_filter
            ),
            Self::Metrics(metrics_cfg) => {
                write!(f, "Successfully set the metrics to {:?}", metrics_cfg)
            }
            Self::MasterStop => write!(f, "stopping main process"),
            // Self::Metrics => write!(f, "Successfully fetched the metrics"),
            Self::NotifiedClient(id) => {
                write!(f, "Successfully notified client {} of the advancement", id)
            }
            Self::PropagatedWorkerEvent => {
                write!(f, "Sent worker response to all subscribing clients")
            }
            Self::Query(_) => write!(f, "Ran the query successfully"),
            Self::ReloadConfiguration(ok, error) => write!(
                f,
                "Successfully reloaded configuration, ok: {}, errors: {}",
                ok, error
            ),
            Self::SaveState(counter, path) => {
                write!(f, "saved {} config messages to {}", counter, path)
            }
            Self::SubscribeEvent(client_id) => {
                write!(f, "Successfully Added {} to subscribers", client_id)
            }
            Self::UpgradeMain(pid) => write!(
                f,
                "new main process launched with pid {}, closing the old one",
                pid
            ),
            Self::UpgradeWorker(id) => {
                write!(f, "Successfully upgraded worker with new id: {}", id)
            }
            Self::WorkerKilled(id) => write!(f, "Successfully killed worker {}", id),
            Self::WorkerLaunched(id) => write!(f, "Successfully launched worker {}", id),
            Self::WorkerOrder(worker) => match worker {
                Some(worker_id) => {
                    write!(f, "Successfully executed the order on worker {}", worker_id)
                }
                None => write!(f, "Successfully executed the order on worker"),
            },
            Self::WorkerResponse => write!(f, "Successfully handled worker response"),
            Self::WorkerRestarted(id) => write!(f, "Successfully restarted worker {}", id),
            Self::WorkerStopped(id) => write!(f, "Successfully stopped worker {}", id),
        }
    }
}

#[derive(Deserialize, Serialize, Debug)]
pub struct ProxyConfiguration {
    id: String,
    state: ConfigState,
}

pub struct CommandServer {
    /// file descriptor of the unix listener socket, usually "sozu.sock"
    unix_listener_fd: i32,
    /// this sender is cloned and distributed around, to send messages back
    command_tx: Sender<CommandMessage>,
    /// where the main loop receives messages
    command_rx: Receiver<CommandMessage>,
    /// All client loops. id -> cloned command_tx
    clients: HashMap<String, Sender<CommandResponse>>,
    /// handles to the workers as seen from the main process
    workers: Vec<Worker>,
    /// A map of requests sent to workers.
    /// Any function requesting a worker will log the request id in here, associated
    /// with a sender. This sender will be used to notify the function of the worker's
    /// response.
    /// In certain cases, the same response may need to be transmitted several
    /// times over. Therefore, a number is recorded next to the sender in
    /// the hashmap.
    in_flight: HashMap<
        String, // the request id
        (
            futures::channel::mpsc::Sender<(ProxyResponse, RequestSummary)>,
            RequestSummary,
        ),
    >,
    event_subscribers: HashSet<String>,
    state: ConfigState,
    config: Config,
    /// id of the next worker to be spawned
    next_worker_id: u32,
    executable_path: String,
    /// caching the number of backends instead of going through the whole state.backends hashmap
    backends_count: usize,
    /// caching the number of frontends instead of going through the whole state.http/hhtps/tcp_fronts hashmaps
    frontends_count: usize,
    accept_cancel: Option<oneshot::Sender<()>>,
}

impl CommandServer {
    fn new(
        fd: i32,
        config: Config,
        command_tx: Sender<CommandMessage>,
        command_rx: Receiver<CommandMessage>,
        mut workers: Vec<Worker>,
        accept_cancel: oneshot::Sender<()>,
    ) -> anyhow::Result<Self> {
        //FIXME
        if config.metrics.is_some() {
            /*METRICS.with(|metrics| {
              if let Some(sock) = (*metrics.borrow_mut()).socket_mut() {
                poll.registry().register(sock, Token(1), Interest::WRITABLE).expect("should register the metrics socket");
              } else {
                error!("could not register metrics socket");
              }
            });*/
        }

        let state: ConfigState = Default::default();

        for worker in workers.iter_mut() {
            let main_to_worker_channel = worker.worker_channel.take().unwrap().sock;
            let (worker_tx, worker_rx) = channel(10000);
            worker.sender = Some(worker_tx);

            let main_to_worker_stream = Async::new(unsafe {
                let fd = main_to_worker_channel.into_raw_fd();
                UnixStream::from_raw_fd(fd)
            })
            .with_context(|| "Could not get a unix stream from the file descriptor")?;

            let id = worker.id;
            let command_tx = command_tx.clone();
            smol::spawn(async move {
                worker_loop(id, main_to_worker_stream, command_tx, worker_rx).await;
            })
            .detach();
        }

        let next_id = workers.len() as u32;
        let executable_path = unsafe { get_executable_path()? };
        let backends_count = state.count_backends();
        let frontends_count = state.count_frontends();

        Ok(CommandServer {
            unix_listener_fd: fd,
            config,
            state,
            command_tx,
            command_rx,
            clients: HashMap::new(),
            workers,
            event_subscribers: HashSet::new(),
            in_flight: HashMap::new(),
            next_worker_id: next_id,
            executable_path,
            backends_count,
            frontends_count,
            accept_cancel: Some(accept_cancel),
        })
    }

    pub async fn run(&mut self) {
        while let Some(command) = self.command_rx.next().await {
            let result: anyhow::Result<Success> = match command {
                CommandMessage::ClientNew { id, sender } => {
                    // this appears twice, which is weird
                    debug!("adding new client {}", id);
                    self.clients.insert(id.to_owned(), sender);
                    Ok(Success::ClientNew(id))
                }
                CommandMessage::ClientClose { id } => {
                    debug!("removing client {}", id);
                    self.clients.remove(&id);
                    self.event_subscribers.remove(&id);
                    Ok(Success::ClientClose(id))
                }
                CommandMessage::ClientRequest { id, message } => {
                    self.handle_client_request(id, message).await
                }
                CommandMessage::WorkerClose { id } => self
                    .handle_worker_close(id)
                    .await
                    .with_context(|| "Could not close worker"),
                CommandMessage::WorkerResponse { id, message } => self
                    .handle_worker_response(id, message)
                    .await
                    .with_context(|| "Could not handle worker response"),
                CommandMessage::Advancement {
                    request_identifier,
                    response,
                } => {
                    self.notify_advancement_to_client(request_identifier, response)
                        .await
                }
                CommandMessage::MasterStop => {
                    info!("stopping main process");
                    Ok(Success::MasterStop)
                }
            };

            match result {
                Ok(order_success) => {
                    trace!("Order OK: {}", order_success);

                    // perform shutdowns
                    match order_success {
                        Success::UpgradeMain(_) => {
                            // the main process has to shutdown after the other has launched successfully
                            //FIXME: should do some cleanup before exiting
                            std::thread::sleep(std::time::Duration::from_secs(2));
                            std::process::exit(0);
                        }
                        Success::MasterStop => {
                            // breaking the loop brings run() to return and ends Sōzu
                            // shouldn't we have the same break for both shutdowns?
                            break;
                        }
                        _ => {}
                    }
                }
                Err(error) => {
                    // log the error on the main process without stopping it
                    error!("Failed order: {:#?}", error);
                }
            }
        }
    }

    pub fn generate_upgrade_data(&self) -> UpgradeData {
        let workers: Vec<SerializedWorker> = self
            .workers
            .iter()
            .map(|ref worker| SerializedWorker::from_worker(worker))
            .collect();
        //FIXME: ensure there's at least one worker
        let state = self.state.clone();

        UpgradeData {
            command_socket_fd: self.unix_listener_fd,
            config: self.config.clone(),
            workers,
            state,
            next_id: self.next_worker_id,
            //token_count: self.token_count,
        }
    }

    pub fn from_upgrade_data(upgrade_data: UpgradeData) -> anyhow::Result<CommandServer> {
        let UpgradeData {
            command_socket_fd: command,
            config,
            workers,
            state,
            next_id,
        } = upgrade_data;

        debug!("listener is: {}", command);
        let listener = Async::new(unsafe { UnixListener::from_raw_fd(command) })?;

        let (accept_cancel_tx, accept_cancel_rx) = oneshot::channel();
        let (command_tx, command_rx) = channel(10000);
        let mut tx = command_tx.clone();

        smol::spawn(async move {
            let mut counter = 0usize;
            let mut accept_cancel_rx = Some(accept_cancel_rx);
            loop {
                /*let (stream, _) = match futures::future::select(
                accept_cancel_rx.take().unwrap(),
                listener.read_with(|l| l.accept())
                ).await {
                */
                let accept_client = listener.accept();
                futures::pin_mut!(accept_client);
                let (stream, _) =
                    match futures::future::select(accept_cancel_rx.take().unwrap(), accept_client)
                        .await
                    {
                        futures::future::Either::Left((_canceled, _)) => {
                            info!("stopping listener");
                            break;
                        }
                        futures::future::Either::Right((res, cancel_rx)) => {
                            accept_cancel_rx = Some(cancel_rx);
                            res.unwrap()
                        }
                    };
                debug!("Accepted a client from upgraded");

                let (client_tx, client_rx) = channel(10000);
                let id = format!("CL-up-{}", counter);
                smol::spawn(client_loop(id.clone(), stream, tx.clone(), client_rx)).detach();
                tx.send(CommandMessage::ClientNew {
                    id,
                    sender: client_tx,
                })
                .await
                .unwrap();
                counter += 1;
            }
        })
        .detach();

        let tx = command_tx.clone();

        let workers: Vec<Worker> = workers
            .iter()
            .filter_map(move |serialized| {
                if serialized.run_state == RunState::Stopped
                    || serialized.run_state == RunState::Stopping
                {
                    return None;
                }

                let (worker_tx, worker_rx) = channel(10000);
                let sender = Some(worker_tx);

                debug!("deserializing worker: {:?}", serialized);
                let stream = Async::new(unsafe { UnixStream::from_raw_fd(serialized.fd) }).unwrap();

                let id = serialized.id;
                let command_tx = tx.clone();
                //async fn worker(id: u32, sock: Async<UnixStream>, tx: Sender<CommandMessage>, rx: Receiver<()>) -> std::io::Result<()> {
                smol::spawn(async move {
                    worker_loop(id, stream, command_tx, worker_rx).await;
                })
                .detach();

                Some(Worker {
                    worker_channel_fd: serialized.fd,
                    id: serialized.id,
                    worker_channel: None,
                    sender,
                    pid: serialized.pid,
                    run_state: serialized.run_state.clone(),
                    queue: serialized.queue.clone().into(),
                    scm_socket: ScmSocket::new(serialized.scm),
                })
            })
            .collect();

        let config_state = state.clone();

        let backends_count = config_state.count_backends();
        let frontends_count = config_state.count_frontends();

        let executable_path = unsafe { get_executable_path()? };

        Ok(CommandServer {
            unix_listener_fd: command,
            config,
            state,
            command_tx,
            command_rx,
            clients: HashMap::new(),
            workers,
            event_subscribers: HashSet::new(),
            in_flight: HashMap::new(),
            next_worker_id: next_id,
            executable_path,
            backends_count,
            frontends_count,
            accept_cancel: Some(accept_cancel_tx),
        })
    }

    pub fn disable_cloexec_before_upgrade(&mut self) -> anyhow::Result<()> {
        for ref mut worker in self.workers.iter_mut() {
            if worker.run_state == RunState::Running {
                let _ = util::disable_close_on_exec(worker.worker_channel_fd).map_err(|e| {
                    error!(
                        "could not disable close on exec for worker {}: {}",
                        worker.id, e
                    );
                });
            }
        }
        trace!(
            "disabling cloexec on listener with file descriptor: {}",
            self.unix_listener_fd
        );
        util::disable_close_on_exec(self.unix_listener_fd)?;
        Ok(())
    }

    pub fn enable_cloexec_after_upgrade(&mut self) -> anyhow::Result<()> {
        for ref mut worker in self.workers.iter_mut() {
            if worker.run_state == RunState::Running {
                let _ = util::enable_close_on_exec(worker.worker_channel_fd).map_err(|e| {
                    error!(
                        "could not enable close on exec for worker {}: {}",
                        worker.id, e
                    );
                });
            }
        }
        util::enable_close_on_exec(self.unix_listener_fd)?;
        Ok(())
    }

    pub async fn load_static_cluster_configuration(&mut self) {
        let (tx, mut rx) = futures::channel::mpsc::channel(self.workers.len() * 2);

        let mut total_message_count = 0usize;

        //FIXME: too many loops, this could be cleaner
        for message in self.config.generate_config_messages() {
            if let CommandRequestOrder::Proxy(order) = message.order {
                self.state.handle_order(&order);

                if let &ProxyRequestOrder::AddCertificate(_) = &order {
                    debug!("config generated AddCertificate( ... )");
                } else {
                    debug!("config generated {:?}", order);
                }

                let mut count = 0usize;
                for ref mut worker in self.workers.iter_mut().filter(|worker| {
                    worker.run_state != RunState::Stopping && worker.run_state != RunState::Stopped
                }) {
                    let request_summary =
                        RequestSummary::new(worker.id, message.id.clone(), None, 1);

                    worker.send(message.id.clone(), order.clone()).await;

                    self.in_flight
                        .insert(message.id.clone(), (tx.clone(), request_summary));
                    count += 1;
                }

                if count == 0 {
                    // FIXME: should send back error here
                    error!("no worker found");
                } else {
                    // self.in_flight
                    //     .insert(message.id.clone(), (tx.clone(), count));
                    total_message_count += count;
                }
            }
        }

        self.backends_count = self.state.count_backends();
        self.frontends_count = self.state.count_frontends();
        gauge!("configuration.clusters", self.state.clusters.len());
        gauge!("configuration.backends", self.backends_count);
        gauge!("configuration.frontends", self.frontends_count);

        smol::spawn(async move {
            let mut ok = 0usize;
            let mut error = 0usize;

            let mut i = 0;
            while let Some((proxy_response, request_summary)) = rx.next().await {
                match proxy_response.status {
                    ProxyResponseStatus::Ok => {
                        ok += 1;
                    }
                    ProxyResponseStatus::Processing => {
                        //info!("metrics processing");
                        continue;
                    }
                    ProxyResponseStatus::Error(e) => {
                        error!(
                            "error handling configuration message {}: {}",
                            proxy_response.id, e
                        );
                        error += 1;
                    }
                };

                i += 1;
                if i == total_message_count {
                    break;
                }
            }

            if error == 0 {
                info!("loading state: {} ok messages, 0 errors", ok);
            } else {
                error!("loading state: {} ok messages, {} errors", ok, error);
            }
        })
        .detach();
    }

    /// in case a worker has crashed while Running and automatic_worker_restart is set to true
    pub async fn restart_worker(&mut self, worker_id: u32) -> anyhow::Result<()> {
        let ref mut worker = self
            .workers
            .get_mut(worker_id as usize)
            .with_context(|| "there should be a worker at that token")?;

        match kill(Pid::from_raw(worker.pid), None) {
            Ok(_) => {
                error!(
                    "worker process {} (PID = {}) is alive but the worker must have crashed. Killing and replacing",
                    worker.id, worker.pid
                );
            }
            Err(_) => {
                error!(
                    "worker process {} (PID = {}) not answering, killing and replacing",
                    worker.id, worker.pid
                );
            }
        }

        kill(Pid::from_raw(worker.pid), Signal::SIGKILL)
            .with_context(|| "failed to kill the worker process")?;

        worker.run_state = RunState::Stopped;

        incr!("worker_restart");

        let id = self.next_worker_id;
        let listeners = Some(Listeners {
            http: Vec::new(),
            tls: Vec::new(),
            tcp: Vec::new(),
        });

        let mut worker = start_worker(
            id,
            &self.config,
            self.executable_path.clone(),
            &self.state,
            listeners,
        )
        .with_context(|| format!("Could not start new worker {}", id))?;

        info!("created new worker: {}", id);
        self.next_worker_id += 1;

        let sock = worker.worker_channel.take().unwrap().sock;
        let (worker_tx, worker_rx) = channel(10_000);
        worker.sender = Some(worker_tx);

        let stream = Async::new(unsafe {
            let fd = sock.into_raw_fd();
            UnixStream::from_raw_fd(fd)
        })?;

        let id = worker.id;
        let command_tx = self.command_tx.clone();
        smol::spawn(async move {
            worker_loop(id, stream, command_tx, worker_rx).await;
        })
        .detach();

        let mut count = 0usize;
        let mut orders = self.state.generate_activate_orders();
        for order in orders.drain(..) {
            worker
                .send(format!("RESTART-{}-ACTIVATE-{}", id, count), order)
                .await;
            count += 1;
        }

        worker
            .send(format!("RESTART-{}-STATUS", id), ProxyRequestOrder::Status)
            .await;

        self.workers.push(worker);

        Ok(())
    }

    async fn handle_worker_close(&mut self, id: u32) -> anyhow::Result<Success> {
        info!("removing worker {}", id);

        if let Some(worker) = self.workers.iter_mut().filter(|w| w.id == id).next() {
            // In case a worker crashes and should be restarted
            if self.config.worker_automatic_restart && worker.run_state == RunState::Running {
                info!("Automatically restarting worker {}", id);
                match self.restart_worker(id).await {
                    Ok(()) => info!("Worker {} has automatically restarted!", id),
                    Err(e) => error!("Could not restart worker {}: {}", id, e),
                }
                return Ok(Success::WorkerRestarted(id));
            }

            info!("Closing the worker {}.", worker.id);
            if !worker.the_pid_is_alive() {
                info!("Worker {} is dead, setting to Stopped.", worker.id);
                worker.run_state = RunState::Stopped;
                return Ok(Success::WorkerStopped(id));
            }

            info!(
                "Worker {} is not dead but should be. Let's kill it.",
                worker.id
            );

            match kill(Pid::from_raw(worker.pid), Signal::SIGKILL) {
                Ok(()) => {
                    info!("Worker {} was successfuly killed", id);
                    worker.run_state = RunState::Stopped;
                    return Ok(Success::WorkerKilled(id));
                }
                Err(e) => {
                    return Err(e).with_context(|| "failed to kill the worker process");
                }
            }
        }
        bail!(format!("Could not find worker {}", id))
    }

    async fn handle_worker_response(
        &mut self,
        id: u32,
        proxy_response: ProxyResponse,
    ) -> anyhow::Result<Success> {
        // Notify the client with Processing in case of a proxy event
        if let Some(ProxyResponseContent::Event(proxy_event)) = proxy_response.content {
            let event: Event = proxy_event.into();
            for client_id in self.event_subscribers.iter() {
                if let Some(client_tx) = self.clients.get_mut(client_id) {
                    let event = CommandResponse::new(
                        proxy_response.id.to_string(),
                        CommandStatus::Processing,
                        format!("{}", id),
                        Some(CommandResponseContent::Event(event.clone())),
                    );
                    client_tx.send(event).await.with_context(|| {
                        format!("could not send message to client {}", client_id)
                    })?
                }
            }
            return Ok(Success::PropagatedWorkerEvent);
        }

        // Notify the function that sent the request to which the worker responded.
        // The in_flight map contains the id of each sent request, together with a sender
        // we use to send the response to.
        match self.in_flight.remove(&proxy_response.id) {
            None => {
                // FIXME: this messsage happens a lot at startup because AddCluster
                // messages receive responses from each of the HTTP, HTTPS and TCP
                // proxys. The clusters list should be merged
                debug!("unknown message id: {}", proxy_response.id);
            }
            // Some((mut requester_tx, mut nb)) => {
            Some((mut sender, mut request_summary)) => {
                let response_id = proxy_response.id.clone();

                // if a worker returned Ok or Error, we're not expecting any more
                // messages with this id from it
                match proxy_response.status {
                    ProxyResponseStatus::Ok | ProxyResponseStatus::Error(_) => {
                        request_summary.expected_responses -= 1;
                    }
                    _ => {}
                };

                // send the proxy response to the thread that sent the request
                if sender
                    .send((proxy_response.clone(), request_summary.clone()))
                    .await
                    .is_err()
                {
                    error!("Failed to send worker response back: {}", proxy_response);
                };

                // reinsert the message_id and sender into the hashmap, for later reuse
                if request_summary.expected_responses > 0 {
                    self.in_flight
                        .insert(response_id, (sender, request_summary));
                }
            }
        }
        Ok(Success::WorkerResponse)
    }
}

pub fn start_server(
    config: Config,
    command_socket_path: String,
    workers: Vec<Worker>,
) -> anyhow::Result<()> {
    let path = PathBuf::from(&command_socket_path);

    if fs::metadata(&path).is_ok() {
        info!("A socket is already present. Deleting...");
        fs::remove_file(&path)
            .with_context(|| format!("could not delete previous socket at {:?}", path))?;
    }

    let unix_listener = match UnixListener::bind(&path) {
        Ok(unix_listener) => unix_listener,
        Err(e) => {
            error!("could not create unix socket: {:?}", e);
            // the workers did not even get the configuration, we can kill them right away
            for worker in workers {
                error!("killing worker n°{} (PID {})", worker.id, worker.pid);
                let _ = kill(Pid::from_raw(worker.pid), Signal::SIGKILL).map_err(|e| {
                    error!("could not kill worker: {:?}", e);
                });
            }
            bail!("couldn't start server");
        }
    };

    if let Err(e) = fs::set_permissions(&path, fs::Permissions::from_mode(0o600)) {
        error!("could not set the unix socket permissions: {:?}", e);
        let _ = fs::remove_file(&path).map_err(|e2| {
            error!("could not remove the unix socket: {:?}", e2);
        });
        // the workers did not even get the configuration, we can kill them right away
        for worker in workers {
            error!("killing worker n°{} (PID {})", worker.id, worker.pid);
            let _ = kill(Pid::from_raw(worker.pid), Signal::SIGKILL).map_err(|e| {
                error!("could not kill worker: {:?}", e);
            });
        }
        bail!("couldn't start server");
    }

    future::block_on(async {
        // Create a listener.
        let listener_fd = unix_listener.as_raw_fd();
        let async_listener = Async::new(unix_listener)?;
        info!("Listening on {:?}", async_listener.get_ref().local_addr()?);

        let mut counter = 0usize;
        let (accept_cancel_tx, accept_cancel_rx) = oneshot::channel();
        let (command_tx, command_rx) = channel(10000);
        let mut cloned_command_tx = command_tx.clone();
        smol::spawn(async move {
            let mut accept_cancel_rx = Some(accept_cancel_rx);

            // create client loops whenever a client connects to the socket
            loop {
                let accept_client = async_listener.accept();
                futures::pin_mut!(accept_client);
                let (stream, _) =
                    match futures::future::select(accept_cancel_rx.take().unwrap(), accept_client)
                        .await
                    {
                        futures::future::Either::Left((_canceled, _)) => {
                            info!("stopping listener");
                            break;
                        }
                        futures::future::Either::Right((result, cancel_rx)) => {
                            accept_cancel_rx = Some(cancel_rx);
                            result.unwrap()
                        }
                    };

                let (client_tx, client_rx) = channel(10000);
                let id = format!("CL-{}", counter);
                smol::spawn(client_loop(
                    id.clone(),
                    stream,
                    cloned_command_tx.clone(),
                    client_rx,
                ))
                .detach();
                cloned_command_tx
                    .send(CommandMessage::ClientNew {
                        id,
                        sender: client_tx,
                    })
                    .await
                    .unwrap();
                counter += 1;
            }
        })
        .detach();

        let saved_state_path = config.saved_state.clone();

        let mut server = CommandServer::new(
            listener_fd,
            config,
            command_tx,
            command_rx,
            workers,
            accept_cancel_tx,
        )?;
        server.load_static_cluster_configuration().await;

        if let Some(path) = saved_state_path {
            server
                .load_state(None, "INITIALIZATION".to_string(), &path)
                .await
                .with_context(|| format!("Loading {:?} failed", &path))?;
        }
        gauge!("configuration.clusters", server.state.clusters.len());
        gauge!("configuration.backends", server.backends_count);
        gauge!("configuration.frontends", server.frontends_count);

        info!("waiting for configuration client connections");
        server.run().await;
        info!("main process stopped");
        Ok(())
    })
}

// The client loop does two things:
// - write everything destined to the client onto the unix stream
// - parse CommandRequests from the unix stream and send them to the command server
async fn client_loop(
    id: String,
    stream: Async<UnixStream>,
    mut command_tx: Sender<CommandMessage>,
    mut client_rx: Receiver<CommandResponse>,
) {
    let read_stream = Arc::new(stream);
    let mut write_stream = read_stream.clone();

    smol::spawn(async move {
        while let Some(response) = client_rx.next().await {
            //info!("sending back message to client: {:?}", msg);
            let mut message: Vec<u8> = serde_json::to_string(&response)
                .map(|string| string.into_bytes())
                .unwrap_or_else(|_| Vec::new());

            // separate all messages with a 0 byte
            message.push(0);
            let _ = write_stream.write_all(&message).await;
        }
    })
    .detach();

    debug!("will start receiving messages from client {}", id);

    // Read the stream by splitting it on 0 bytes
    let mut split_iterator = BufReader::new(read_stream).split(0);
    while let Some(message) = split_iterator.next().await {
        let message = match message {
            Err(e) => {
                error!("could not split message: {:?}", e);
                break;
            }
            Ok(msg) => msg,
        };

        match serde_json::from_slice::<CommandRequest>(&message) {
            Err(e) => {
                error!("could not decode client message: {:?}", e);
                break;
            }
            Ok(command_request) => {
                debug!("got command request: {:?}", command_request);
                let id = id.clone();
                if let Err(e) = command_tx
                    .send(CommandMessage::ClientRequest {
                        id,
                        message: command_request,
                    })
                    .await
                {
                    error!("error sending client request to command server: {:?}", e);
                }
            }
        }
    }

    // If the loop breaks, order the command server to close the client
    if let Err(send_error) = command_tx
        .send(CommandMessage::ClientClose { id: id.to_owned() })
        .await
    {
        error!(
            "The client loop {} could not send ClientClose to the command server: {:?}",
            id, send_error
        );
    }
}

// the worker loop does two things:
// - write everything destined to the worker onto the unix stream
// - parse ProxyResponses from the unix stream and send them to the CommandServer
async fn worker_loop(
    id: u32,
    stream: Async<UnixStream>,
    mut command_tx: Sender<CommandMessage>,
    mut worker_rx: Receiver<ProxyRequest>,
) {
    let read_stream = Arc::new(stream);
    let mut write_stream = read_stream.clone();

    smol::spawn(async move {
        debug!("will start sending messages to worker {}", id);
        while let Some(request) = worker_rx.next().await {
            debug!("sending to worker {}: {:?}", id, request);
            let mut message: Vec<u8> = serde_json::to_string(&request)
                .map(|string| string.into_bytes())
                .unwrap_or_else(|_| Vec::new());

            // separate all messages with a 0 byte
            message.push(0);
            let _ = write_stream.write_all(&message).await;
        }
    })
    .detach();

    debug!("will start receiving messages from worker {}", id);

    // Read the stream by splitting it on 0 bytes
    let mut split_iterator = BufReader::new(read_stream).split(0);
    while let Some(message) = split_iterator.next().await {
        let message = match message {
            Err(e) => {
                error!("could not split message: {:?}", e);
                break;
            }
            Ok(msg) => msg,
        };

        match serde_json::from_slice::<ProxyResponse>(&message) {
            Err(e) => {
                error!("could not decode worker message: {:?}", e);
                break;
            }
            Ok(proxy_response) => {
                debug!("worker {} replied message: {:?}", id, proxy_response);
                let id = id.clone();
                if let Err(e) = command_tx
                    .send(CommandMessage::WorkerResponse {
                        id,
                        message: proxy_response,
                    })
                    .await
                {
                    error!("error sending worker response to command server: {:?}", e);
                }
            }
        }
    }

    // if the loop breaks, order the command server to close the worker
    if let Err(send_error) = command_tx
        .send(CommandMessage::WorkerClose { id: id.to_owned() })
        .await
    {
        error!(
            "The worker loop {} could not send WorkerClose to the CommandServer: {:?}",
            id, send_error
        );
    }
}

/// keeps track of a request as handled by the CommandServer
#[derive(Clone, Debug, Serialize)]
pub struct RequestSummary {
    /// the worker we send the request to
    pub worker_id: u32,
    /// the request id as sent within ProxyRequest
    pub request_id: String,
    /// the client who sent the request
    pub client: Option<String>,
    /// In certain cases, the same response may need to be transmitted several times over
    pub expected_responses: usize,
}

impl RequestSummary {
    pub fn new(
        worker_id: u32,
        request_id: String,
        client: Option<String>,
        expected_responses: usize,
    ) -> Self {
        Self {
            worker_id,
            request_id,
            client,
            expected_responses,
        }
    }
}
