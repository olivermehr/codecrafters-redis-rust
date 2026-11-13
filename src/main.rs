#![allow(unused_imports)]
use std::{
    io::{BufReader, Read, Write},
    net::TcpListener,
};

use tokio::net::TcpSocket;

#[tokio::main]
async fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();

    for stream in listener.incoming() {
        match stream {
            Ok(mut stream) => {
                tokio::spawn(async move {
                    println!("spawing connection");
                    let mut buf = [0; 128];
                    while let Ok(bytes) = stream.read(&mut buf)
                        && bytes != 0
                    {
                        let s = String::from_utf8(buf.to_vec())
                            .unwrap()
                            .to_ascii_lowercase();
                        println!("{s}");
                        for _ in s.split("*1\r\n$4\r\nPING\r\n") {
                            let _ = stream.write_all(&String::from("+PONG\r\n").into_bytes());
                        }
                    }
                });
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}
