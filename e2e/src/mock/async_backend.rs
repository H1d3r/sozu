use std::{
    io::{ErrorKind, Read, Write},
    net::{SocketAddr, TcpListener, TcpStream},
    thread,
};

use futures::channel::mpsc;

use crate::{mock::aggregator::{Aggregator, SimpleAggregator}, http::http_response};

/// Handle to a detached thread where a TcpListener runs
pub struct Backend<T> {
    pub name: String,
    pub stop_tx: mpsc::Sender<()>,
    pub aggregator_rx: mpsc::Receiver<T>,
}

impl<T: Aggregator + Send + Sync + 'static> Backend<T> {
    pub fn new<S: Into<String>>(
        name: S,
        address: SocketAddr,
        mut aggregator: T,
        handler: Box<dyn Fn(&TcpStream, T) -> T + Send + Sync>,
    ) -> Self {
        let name = name.into();
        let (stop_tx, mut stop_rx) = mpsc::channel::<()>(1);
        let (mut aggregator_tx, aggregator_rx) = mpsc::channel::<T>(1);
        let listener = TcpListener::bind(address).expect("could not bind");
        let mut clients = Vec::new();
        let thread_name = name.to_owned();
        thread::spawn(move || {
            listener
                .set_nonblocking(true)
                .expect("could not set nonblocking on listener");
            loop {
                let stream = listener.accept();
                match stream {
                    Ok(stream) => {
                        println!("{}: new connection", thread_name);
                        stream
                            .0
                            .set_nonblocking(true)
                            .expect("cound not set nonblocking on client");
                        clients.push(stream.0);
                    }
                    Err(error) => {
                        if error.kind() != ErrorKind::WouldBlock {
                            println!("IO Error: {:?}", error);
                        }
                    }
                }
                for client in &clients {
                    aggregator = handler(client, aggregator);
                }
                match stop_rx.try_next() {
                    Ok(Some(_)) => break,
                    _ => continue,
                }
            }
            aggregator_tx
                .try_send(aggregator)
                .expect("could not send aggregator");
        });
        Self {
            name,
            stop_tx,
            aggregator_rx,
        }
    }
    pub fn stop_and_get_aggregator(&mut self) -> Option<T> {
        self.stop_tx.try_send(()).expect("could not stop backend");
        loop {
            match self.aggregator_rx.try_next() {
                Ok(Some(aggregator)) => return Some(aggregator),
                _ => continue,
            }
        }
    }
}

impl Backend<SimpleAggregator> {
    pub fn http_handler<S: Into<String>>(
        content: S,
    ) -> Box<dyn Fn(&TcpStream, SimpleAggregator) -> SimpleAggregator + Send + Sync> {
        let content = content.into();
        Box::new(
            move |mut stream: &TcpStream, mut aggregator: SimpleAggregator| {
                let mut buf = [0u8; 4096];
                match stream.read(&mut buf) {
                    Ok(0) => return aggregator,
                    Ok(n) => {
                        println!("{} received {}", content, n);
                    }
                    Err(_) => {
                        //println!("{} could not receive {}", content, error);
                        return aggregator;
                    }
                }
                aggregator.received += 1;
                let response = http_response(&content);
                stream.write_all(response.as_bytes()).unwrap();
                aggregator.sent += 1;
                aggregator
            },
        )
    }
    pub fn tcp_handler<S: Into<String>>(
        content: String,
    ) -> Box<dyn Fn(&TcpStream, SimpleAggregator) -> SimpleAggregator + Send + Sync> {
        let content: String = content.into();
        Box::new(
            move |mut stream: &TcpStream, mut aggregator: SimpleAggregator| {
                let mut buf = [0u8; 4096];
                match stream.read(&mut buf) {
                    Ok(0) => return aggregator,
                    Ok(n) => {
                        println!("{} received {}", content, n);
                    }
                    Err(error) => {
                        println!("{} could not receive {}", content, error);
                        return aggregator;
                    }
                }
                stream.write_all(content.as_bytes()).unwrap();
                aggregator.sent += 1;
                aggregator
            },
        )
    }
}
