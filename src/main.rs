#![allow(unused_imports)]

use std::{
    borrow::Cow,
    collections::HashMap,
    sync::{Arc, Mutex},
};

use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpSocket},
};

use crate::resp::parser::{RespNode, RespParser};
mod resp;

#[derive(Debug, Clone)]
struct RespServer {
    storage: Arc<Mutex<HashMap<String, String>>>,
}

impl RespServer {
    fn new() -> Self {
        Self {
            storage: Arc::new(Mutex::new(HashMap::new())),
        }
    }
    fn visit(&mut self, node: &RespNode) -> Option<String> {
        match node {
            RespNode::Array(v) => {
                let mut v = v.into_iter();
                if let Some(fn_name) = v.next() {
                    let fn_name = self.visit(fn_name).unwrap();
                    if fn_name == "ping" {
                        Some(format_simple_string(String::from("PONG")))
                    } else if fn_name == "echo"
                        && let Some(param) = v.next()
                    {
                        let value = self.visit(param).unwrap();
                        Some(format_bulk_string(value))
                    } else if fn_name == "set" {
                        let key = v.next();
                        let value = v.next();
                        if let (Some(RespNode::BulkString(k)), Some(RespNode::BulkString(v))) =
                            (key, value)
                        {
                            self.storage.lock().unwrap().insert(k.clone(), v.clone());
                            Some(format_simple_string(String::from("Ok")))
                        } else {
                            None
                        }
                    } else if fn_name == "get" {
                        println!("{:?}", self.storage);
                        let key = v.next();
                        if let Some(RespNode::BulkString(s)) = key {
                            println!("{s}");
                            let value = self.storage.lock().unwrap().get(s).cloned();
                            if let Some(v) = value {
                                println!("{v}");
                                Some(format_bulk_string(v))
                            } else {
                                Some(null_string())
                            }
                        } else {
                            None
                        }
                    } else {
                        panic!("unsupported fn name")
                    }
                } else {
                    None
                }
            }
            RespNode::BulkString(s) => Some(s.clone()),
            RespNode::SimpleString(s) => Some(s.clone()),
        }
    }
}

#[tokio::main]
async fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();
    let server = RespServer::new();
    loop {
        match listener.accept().await {
            Ok((mut stream, _)) => {
                let mut cloned_server = server.clone();
                tokio::spawn(async move {
                    let mut buf = [0; 128];
                    while let Ok(bytes) = stream.read(&mut buf).await
                        && bytes != 0
                    {
                        let s = String::from_utf8_lossy(&buf[..bytes])
                            .into_owned()
                            .to_lowercase();
                        let mut parser = RespParser::new(s.chars().collect());
                        let tree = parser.create_tree();

                        match tree {
                            Ok(nodes) => {
                                for node in nodes.iter() {
                                    match cloned_server.visit(node) {
                                        Some(s) => {
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
fn null_string() -> String {
    String::from("$-1\r\n")
}
