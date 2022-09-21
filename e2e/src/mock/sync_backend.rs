use std::{
    collections::HashMap,
    io::{Read, Write},
    net::{SocketAddr, TcpListener, TcpStream},
    str::from_utf8,
};

pub struct Backend {
    pub name: String,
    pub address: SocketAddr,
    pub listener: Option<TcpListener>,
    pub clients: HashMap<usize, TcpStream>,
    pub response: String,
    pub sent: usize,
    pub received: usize,
}

impl Backend {
    pub fn new<S1: Into<String>, S2: Into<String>>(
        name: S1,
        address: SocketAddr,
        response: S2,
    ) -> Self {
        let name = name.into();
        let response = response.into();
        Self {
            name,
            address,
            listener: None,
            clients: HashMap::new(),
            response,
            sent: 0,
            received: 0,
        }
    }
    pub fn connect(&mut self) {
        let listener = TcpListener::bind(self.address).expect("could not bind");
        self.listener = Some(listener);
        self.clients = HashMap::new();
    }
    pub fn disconnect(&mut self) {
        self.listener = None;
        self.clients = HashMap::new();
    }
    pub fn accept(&mut self, id: usize) -> bool {
        if let Some(listener) = &self.listener {
            let stream = listener.accept();
            match stream {
                Ok(stream) => {
                    self.clients.insert(id, stream.0);
                    return true;
                }
                Err(error) => {
                    println!("accept error: {:?}", error);
                }
            }
        }
        false
    }
    pub fn send(&mut self, id: usize) -> Option<usize> {
        match self.clients.get_mut(&id) {
            Some(stream) => match stream.write(self.response.as_bytes()) {
                Ok(0) => {
                    println!("{} received nothing", self.name);
                }
                Ok(n) => {
                    println!("{} sent {} to {}", self.name, n, id);
                    self.sent += 1;
                    return Some(n);
                }
                Err(error) => {
                    println!("{} could not respond to {}: {}", self.name, id, error);
                }
            },
            None => {
                println!("no client with id {} on backend {}", id, self.name);
            }
        }
        None
    }
    pub fn receive(&mut self, id: usize) -> Option<String> {
        match self.clients.get_mut(&id) {
            Some(stream) => {
                let mut buf = [0u8; 4096];
                match stream.read(&mut buf) {
                    Ok(0) => {
                        println!("{} sent nothing", self.name);
                    }
                    Ok(n) => {
                        println!("{} received {} from {}", self.name, n, id);
                        self.received += 1;
                        return Some(from_utf8(&buf[..n]).unwrap().to_string());
                    }
                    Err(error) => {
                        println!("{} could not receive for {}: {}", self.name, id, error);
                    }
                }
            }
            None => {
                println!("no client with id {} on backend {}", id, self.name);
            }
        }
        None
    }
}
