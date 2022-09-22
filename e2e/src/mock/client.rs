use std::{
    io::{Read, Write},
    net::{SocketAddr, TcpStream},
    str::from_utf8,
};

/// Wrapper over a TCP connection
pub struct Client {
    pub name: String,
    pub address: SocketAddr,
    pub stream: Option<TcpStream>,
    pub request: String,
    pub sent: usize,
    pub received: usize,
}

impl Client {
    pub fn new<S1: Into<String>, S2: Into<String>>(
        name: S1,
        address: SocketAddr,
        request: S2,
    ) -> Self {
        let name = name.into();
        let request = request.into();
        Self {
            name,
            address,
            stream: None,
            request,
            sent: 0,
            received: 0,
        }
    }
    pub fn connect(&mut self) {
        let stream = TcpStream::connect(self.address).expect("could not connect");
        self.stream = Some(stream);
    }
    pub fn disconnect(&mut self) {
        self.stream = None;
    }
    pub fn send(&mut self) -> Option<usize> {
        match self.stream {
            Some(ref mut stream) => match stream.write(&self.request.as_bytes()) {
                Ok(0) => {
                    println!("{} sent nothing", self.name);
                }
                Ok(n) => {
                    println!("{} sent {}", self.name, n);
                    self.sent += 1;
                    return Some(n);
                }
                Err(error) => {
                    println!("{} could not send: {}", self.name, error);
                }
            },
            None => {
                println!("{} is not connected", self.name);
            }
        }
        None
    }
    pub fn receive(&mut self) -> Option<String> {
        match self.stream {
            Some(ref mut stream) => {
                let mut buf = [0u8; 4096];
                match stream.read(&mut buf) {
                    Ok(0) => {
                        println!("{} received nothing", self.name);
                    }
                    Ok(n) => {
                        println!("{} received {}", self.name, n);
                        self.received += 1;
                        return Some(from_utf8(&buf[..n]).unwrap().to_string());
                    }
                    Err(error) => {
                        println!("{} could not receive: {}", self.name, error);
                    }
                }
            }
            None => {
                println!("{} is not connected", self.name);
            }
        }
        None
    }
}
