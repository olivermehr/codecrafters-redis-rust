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
                let mut buf = [0; 128];
                let _ = stream.read(&mut buf).unwrap();
                let s = String::from_utf8(buf.to_vec())
                    .unwrap()
                    .to_ascii_lowercase();
                if s.contains("ping") {
                    println!("entering the ping block");
                    let _ = stream
                        .write(&String::from("+PONG\r\n").into_bytes())
                        .unwrap();
                }
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}
