#![allow(unused_imports)]
use std::{
    io::{Read, Write},
    net::TcpListener,
};

use tokio::net::TcpSocket;

fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();

    for stream in listener.incoming() {
        match stream {
            Ok(mut stream) => {
                let mut s = String::new();
                let msg = stream.read_to_string(&mut s).unwrap();
                println!("{}",s);
                if s == "PING" {
                    let resp = String::from("+PONG\r\n");
                    let bytes = resp.bytes().collect::<Vec<u8>>();
                    stream.write_all(&bytes).unwrap();
                    stream.flush().unwrap();
                    
                }
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}
