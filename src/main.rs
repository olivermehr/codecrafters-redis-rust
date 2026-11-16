#![allow(unused_imports)]

use std::borrow::Cow;

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
                        println!("tree {:?}", tree);

                        match tree {
                            Ok(nodes) => {
                                for node in nodes {
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

fn format_simple_string(mut s: String) -> String {
    s.insert(0, '+');
    s.push_str("\r\n");
    s
}
fn format_bulk_string(mut s: String) -> String {
    let len = s.len();
    s.insert_str(0, &format!("${}\r\n", len));
    s.push_str("\r\n");
    s
}

fn visit<'a>(node: RespNode) -> Option<String> {
    match node {
        RespNode::Array(v) => {
            let mut v = v.into_iter();
            if let Some(fn_name) = v.next() {
                let fn_name = visit(fn_name).unwrap();
                if fn_name == "ping" {
                    Some(format_simple_string(String::from("PONG\r\n")))
                } else if fn_name == "echo"
                    && let Some(param) = v.next()
                {
                    let value = visit(param).unwrap();
                    Some(format_bulk_string(value))
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

#[derive(Debug)]
enum RespResponse {
    SimpleString(String),
}
