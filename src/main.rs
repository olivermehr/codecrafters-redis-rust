#![allow(unused_imports)]
use core::error;
use std::{
    array,
    borrow::Cow,
    collections::{HashMap, VecDeque, btree_map::Entry},
    fmt::Display,
    fs::read,
    ops::Add,
    sync::{Arc, Mutex, MutexGuard},
    time::Duration,
};

use chrono::{DateTime, Utc};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpSocket},
    sync::{
        mpsc::{self, Receiver, Sender},
        oneshot,
    },
};

use crate::resp::{
    error::RespError,
    parser::{RespNode, RespParser},
};
mod commands;
mod resp;

#[derive(Debug, Clone)]
struct RespServer {
    command_queue: Option<Vec<RespNode>>,
    storage: Arc<Mutex<HashMap<String, Storage>>>,
}

#[derive(Debug)]
struct Storage {
    content: StorageType,
    queue: Option<tokio::sync::oneshot::Sender<StorageType>>,
}

impl Storage {
    fn new(content: StorageType) -> Self {
        Self {
            content,
            queue: None,
        }
    }
}

#[derive(Debug, Clone)]
struct StreamEntries {
    id: String,
    stream_key: String,
    data: Vec<(String, String)>,
}

#[derive(Debug, Clone)]
enum StorageType {
    String(String),
    List(VecDeque<StorageType>),
    Stream(Vec<StreamEntries>),
}

impl Display for StorageType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::List(_) => write!(f, "list"),
            Self::String(_) => write!(f, "string"),
            Self::Stream(_) => write!(f, "stream"),
        }
    }
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

static EMPTY_ARRAY: &[String; 0] = &[];

impl RespServer {
    fn new() -> Self {
        Self {
            storage: Arc::new(Mutex::new(HashMap::new())),
            command_queue: None,
        }
    }

    fn handle_bl_pop(
        &self,
        queue: &mut Option<tokio::sync::oneshot::Sender<StorageType>>,
        value: String,
    ) {
        if let Some(tx) = queue.take() {
            tx.send(value.into()).unwrap();
        }
    }

    async fn visit(&mut self, node: RespNode, tx: Sender<Expiry>) -> Result<String, RespError> {
        if let RespNode::Array(v) = node {
            let mut args = v.into_iter();
            let fn_name = get_bulk_string_arg(args.next())?;
            let command = commands::COMMANDS
                .get(fn_name.as_str())
                .expect("unsupported fn name");
            command(self, args, tx).await
        } else {
            Err(RespError::InvalidSyntax)
        }
    }

    fn blpop_front(&mut self, list: &str) -> Option<StorageType> {
        let mut map = self.storage.lock().unwrap();
        let storage = map
            .entry(list.to_string())
            .or_insert(Storage::new(StorageType::List(VecDeque::new())));

        if let StorageType::List(l) = &mut storage.content {
            return l.pop_front();
        }
        None
    }
    fn validate_stream_id(&self, stream_key: &str, id: String) -> Result<String, String> {
        if id == "0-0" {
            Err(String::from(
                "The ID specified in XADD must be greater than 0-0",
            ))?
        }
        let (new_milli, new_seq) = id.split_once('-').unwrap_or(("*", "*"));
        let lock = self.storage.lock().unwrap();

        if let Some(ss) = lock.get(stream_key)
            && let StorageType::Stream(v) = &ss.content
            && let Some(entry) = v.last()
        {
            let (last_milli, last_seq) = entry.id.split_once('-').unwrap();
            let last_milli: usize = last_milli.parse().unwrap();
            let last_seq: usize = last_seq.parse().unwrap();

            let new_milli: usize = if new_milli == "*" {
                Utc::now().timestamp_millis() as usize
            } else {
                new_milli.parse().unwrap()
            };

            let new_seq: usize = if new_seq == "*" {
                if new_milli == 0 {
                    1
                } else if new_milli == last_milli {
                    last_seq + 1
                } else {
                    0
                }
            } else {
                new_seq.parse().unwrap()
            };

            if new_milli < last_milli || (new_milli == last_milli && new_seq <= last_seq) {
                Err(String::from(
                    "The ID specified in XADD is equal or smaller than the target stream top item",
                ))?
            }
            return Ok(format!("{}-{}", new_milli, new_seq));
        }

        let new_milli = if new_milli == "*" {
            Utc::now().timestamp_millis() as usize
        } else {
            new_milli.parse().unwrap()
        };
        let new_seq: usize = if new_seq == "*" {
            if new_milli == 0 { 1 } else { 0 }
        } else {
            new_seq.parse().unwrap()
        };

        Ok(format!("{}-{}", new_milli, new_seq))
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
                    let mut buf = [0; 1024];
                    let mut read_index = 0;
                    while let Ok(bytes) = stream.read(&mut buf[read_index..]).await
                        && bytes != 0
                    {
                        let s = String::from_utf8_lossy(&buf[..read_index + bytes])
                            .into_owned()
                            .to_lowercase();
                        let mut parser = RespParser::new(s.chars().collect());
                        let (bytes_used, tree) = parser.create_tree();
                        if bytes_used >= buf.len() {
                            buf.fill(0);
                        } else {
                            if bytes_used == 0 {
                                read_index = bytes;
                            } else {
                                read_index = 0;
                                buf.copy_within(bytes_used.., 0);
                                let remaining = buf.len() - bytes_used;
                                for i in buf[remaining..].iter_mut() {
                                    *i = 0;
                                }
                            }
                        }
                        for node in tree.into_iter() {
                            match cloned_server.visit(node, tx_clone.clone()).await {
                                Ok(s) => {
                                    stream.write_all(s.as_bytes()).await.unwrap();
                                }
                                Err(e) => {
                                    let error = match e {
                                        RespError::Empty => format_error_string("no arguments"),
                                        RespError::IncorrectParams => {
                                            format_error_string("invalid params")
                                        }
                                        RespError::InvalidSyntax => {
                                            format_error_string("invalid syntax")
                                        }
                                        RespError::IncompleteTcpStream => {
                                            format_error_string("incomplete tcp stream")
                                        }
                                    };
                                    stream.write_all(error.as_bytes()).await.unwrap()
                                }
                            }
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
fn format_bulk_string(mut s: String) -> String {
    let len = s.len();
    s.insert_str(0, &format!("${}\r\n", len));
    s.push_str("\r\n");
    s
}
fn format_null_string() -> String {
    String::from("$-1\r\n")
}

fn format_null_array() -> String {
    String::from("*-1\r\n")
}

fn format_error_string(error: &str) -> String {
    format!("-ERR {error}\r\n")
}

fn format_integer_string(n: isize) -> String {
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

fn format_array(v: &[String]) -> String {
    let len = v.len();
    let mut s = format!("*{len}\r\n");
    if len == 0 {
        return s;
    }
    for el in v {
        s.push_str(el);
    }
    s
}

fn normalise_lrange_indices(len: usize, index: isize) -> usize {
    if index < 0 {
        if index.unsigned_abs() > len {
            return 0;
        }
        len - index.unsigned_abs()
    } else {
        if index as usize > len {
            return len - 1;
        }
        index.unsigned_abs()
    }
}

fn format_stream_entries(iter: &[StreamEntries]) -> String {
    let mut result_strings = Vec::new();

    for e in iter {
        let mut entry_vec = vec![format_bulk_string(e.id.clone())];
        let mut kp_vec = Vec::new();
        for (key, value) in e.data.iter() {
            kp_vec.push(format_bulk_string(key.clone()));
            kp_vec.push(format_bulk_string(value.clone()));
        }
        entry_vec.push(format_array(&kp_vec));
        result_strings.push(format_array(&entry_vec));
    }
    format_array(&result_strings)
}
