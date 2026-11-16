#![allow(unused_imports)]

use core::error;
use std::{
    borrow::Cow,
    collections::HashMap,
    ops::Add,
    sync::{Arc, Mutex, MutexGuard},
    time::Duration,
};

use chrono::{DateTime, Utc};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpSocket},
    sync::mpsc::{self, Receiver, Sender},
};

use crate::resp::{
    error::RespError,
    parser::{RespNode, RespParser},
};
mod resp;

#[derive(Debug, Clone)]
struct RespServer {
    storage: Arc<Mutex<HashMap<String, StorageType>>>,
}

#[derive(Debug, Clone)]
enum StorageType {
    String(String),
    List(Vec<String>),
}

impl From<String> for StorageType {
    fn from(value: String) -> Self {
        Self::String(value)
    }
}
impl From<StorageType> for String {
    fn from(value: StorageType) -> Self {
        match value {
            StorageType::String(s) => s,
            _ => panic!("unsupported string conversion"),
        }
    }
}

impl RespServer {
    fn new() -> Self {
        Self {
            storage: Arc::new(Mutex::new(HashMap::new())),
        }
    }
    async fn visit(&mut self, node: RespNode, tx: Sender<Expiry>) -> Result<String, RespError> {
        if let RespNode::Array(v) = node {
            let mut v = v.into_iter();
            let fn_name = get_bulk_string_arg(v.next())?;
            if fn_name == "ping" {
                Ok(format_simple_string(String::from("PONG")))
            } else if fn_name == "echo" {
                let value = get_bulk_string_arg(v.next())?;
                Ok(format_bulk_string(value).0)
            } else if fn_name == "set" {
                let key = get_bulk_string_arg(v.next())?;
                let value = get_bulk_string_arg(v.next())?;
                while let Some(arg) = get_bulk_string_arg_optional(v.next()) {
                    if arg == "ex" {
                        let num = get_bulk_string_arg(v.next())?;
                        tx.send(Expiry {
                            key: key.clone(),
                            millis: num.parse::<u64>().map_err(|e| RespError::IncorrectParams)?
                                * 1000,
                        })
                        .await
                        .unwrap();
                    } else if arg == "px" {
                        let num = get_bulk_string_arg(v.next())?;
                        tx.send(Expiry {
                            key: key.clone(),
                            millis: num.parse().map_err(|e| RespError::IncorrectParams)?,
                        })
                        .await
                        .unwrap();
                    }
                }
                self.storage.lock().unwrap().insert(key, value.into());
                Ok(format_simple_string(String::from("OK")))
            } else if fn_name == "get" {
                let key = get_bulk_string_arg(v.next())?;
                let value = self.storage.lock().unwrap().get(&key).cloned();
                if let Some(v) = value {
                    Ok(format_bulk_string(v.into()).0)
                } else {
                    Ok(null_string())
                }
            } else if fn_name == "rpush" {
                let key = get_bulk_string_arg(v.next())?;
                let mut values = Vec::new();

                for val in v {
                    if let RespNode::BulkString(s) = val {
                        values.push(s);
                    }
                }
                let mut map = self.storage.lock().unwrap();
                let list = map.entry(key).or_insert(StorageType::List(Vec::new()));

                if let StorageType::List(l) = list {
                    l.extend(values);
                    let new_len = l.len();
                    return Ok(integer_string(new_len as isize));
                }
                panic!("shouldnt reach here")
            } else if fn_name == "lrange" {
                let list_name = get_bulk_string_arg(v.next())?;
                let start_index: usize = get_bulk_string_arg(v.next())?.parse().unwrap();
                let stop_index: usize = get_bulk_string_arg(v.next())?.parse().unwrap();
                let lock = self.storage.lock().unwrap();
                let list = lock.get(&list_name);
                if let Some(StorageType::List(v)) = list {
                    let len = v.len();
                    if start_index >= len || stop_index >= len || start_index > stop_index {
                        Ok(format_array(Vec::new()))
                    } else {
                        let mut bs = Vec::new();
                        for s in v[start_index..=stop_index].iter() {
                            bs.push(format_bulk_string(s.clone()));
                        }
                        Ok(format_array(bs))
                    }
                } else {
                    Ok(format_array(Vec::new()))
                }
            } else {
                panic!("unsupported fn name")
            }
        } else {
            Err(RespError::InvalidSyntax)
        }
    }
}

struct Expiry {
    key: String,
    millis: u64,
}

#[tokio::main]
async fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();
    let server = RespServer::new();
    let (tx, mut rx) = mpsc::channel::<Expiry>(50);
    tokio::spawn({
        let cloned_server = server.clone();
        async move {
            while let Some(expiry) = rx.recv().await {
                tokio::spawn({
                    let spawn_sever = cloned_server.clone();
                    async move {
                        tokio::time::sleep(Duration::from_millis(expiry.millis)).await;
                        loop {
                            if let Ok(mut map) = spawn_sever.storage.try_lock() {
                                map.remove(&expiry.key);
                                break;
                            }
                        }
                    }
                });
            }
        }
    });
    loop {
        match listener.accept().await {
            Ok((mut stream, _)) => {
                let mut cloned_server = server.clone();
                let tx_clone = tx.clone();
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
                                for node in nodes.into_iter() {
                                    match cloned_server.visit(node, tx_clone.clone()).await {
                                        Ok(s) => {
                                            stream.write_all(s.as_bytes()).await.unwrap();
                                        }
                                        Err(e) => {
                                            let error = match e {
                                                RespError::Empty => error_string("no arguments"),
                                                RespError::IncorrectParams => {
                                                    error_string("invalid params")
                                                }
                                                RespError::InvalidSyntax => {
                                                    error_string("invalid syntax")
                                                }
                                            };
                                            stream.write_all(error.as_bytes()).await.unwrap()
                                        }
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

struct BulkString(String);

fn format_simple_string(mut s: String) -> String {
    s.insert(0, '+');
    s.push_str("\r\n");
    s
}
fn format_bulk_string(mut s: String) -> BulkString {
    let len = s.len();
    s.insert_str(0, &format!("${}\r\n", len));
    s.push_str("\r\n");
    BulkString(s)
}
fn null_string() -> String {
    String::from("$-1\r\n")
}

fn error_string(error: &str) -> String {
    format!("-{error}\r\n")
}

fn integer_string(n: isize) -> String {
    format!(":{n}\r\n")
}

fn get_bulk_string_arg(node: Option<RespNode>) -> Result<String, RespError> {
    if let Some(node) = node
        && let RespNode::BulkString(s) = node
    {
        Ok(s)
    } else {
        Err(RespError::IncorrectParams)
    }
}
fn get_bulk_string_arg_optional(node: Option<RespNode>) -> Option<String> {
    if let Some(node) = node
        && let RespNode::BulkString(s) = node
    {
        Some(s)
    } else {
        None
    }
}

fn get_integer_arg(node: Option<RespNode>) -> Result<isize, RespError> {
    if let Some(node) = node
        && let RespNode::Integer(i) = node
    {
        Ok(i)
    } else {
        Err(RespError::IncorrectParams)
    }
}

fn format_array(v: Vec<BulkString>) -> String {
    let len = v.len();
    let mut s = format!("*{len}\r\n");
    if len == 0 {
        return s;
    }
    for el in v {
        s.push_str(&el.0);
    }
    s
}
