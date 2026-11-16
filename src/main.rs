#![allow(unused_imports)]

use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpSocket},
};

use crate::resp::parser::{RespNode, RespParser};

mod resp;

#[tokio::main]
async fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();
    loop {
        match listener.accept().await {
            Ok((mut stream, _)) => {
                tokio::spawn(async move {
                    let mut buf = [0; 128];
                    while let Ok(bytes) = stream.read(&mut buf).await
                        && bytes != 0
                    {
                        let s = String::from_utf8_lossy(&buf[..bytes])
                            .into_owned()
                            .to_lowercase();
                        println!("before parse {s}");
                        let mut parser = RespParser::new(s.chars().collect());
                        let tree = parser.create_tree();
                        println!("tree {:?}",tree);

                        match tree {
                            Ok(nodes) => {
                                for node in nodes.iter() {
                                    match visit(node) {
                                        Some(s) => {
                                            println!("{s}");
                                            stream.write_all(s.as_bytes()).await.unwrap();
                                        }
                                        None => continue,
                                    }
                                }
                            }
                            Err(e) => {}
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

fn visit(node: &RespNode) -> Option<&str> {
    match node {
        RespNode::Array(v) => {
            if let Some(first) = v.first() {
                let fn_name = visit(first).unwrap();
                if fn_name == "ping" {
                    Some("+PONG\r\n")
                } else if fn_name == "echo"
                    && let Some(param) = v.get(1)
                {
                    visit(param)
                } else {
                    panic!("unsupported fn name")
                }
            } else {
                None
            }
        }
        RespNode::BulkString(s) => Some(s),
        RespNode::SimpleString(s) => Some(s),
    }
}
