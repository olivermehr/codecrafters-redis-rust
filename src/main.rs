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
mod resp;

#[derive(Debug, Clone)]
struct RespServer {
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
            let mut v = v.into_iter();
            let fn_name = get_bulk_string_arg(v.next())?;
            if fn_name == "ping" {
                Ok(format_simple_string(String::from("PONG")))
            } else if fn_name == "echo" {
                let value = get_bulk_string_arg(v.next())?;
                Ok(format_bulk_string(value))
            } else if fn_name == "set" {
                let key = get_bulk_string_arg(v.next())?;
                let value = get_bulk_string_arg(v.next())?;
                while let Some(arg) = get_bulk_string_arg_optional(v.next()) {
                    if arg == "ex" {
                        let num = get_bulk_string_arg(v.next())?;
                        tx.send(Expiry {
                            key: key.clone(),
                            millis: num.parse::<u64>().map_err(|_| RespError::IncorrectParams)?
                                * 1000,
                        })
                        .await
                        .unwrap();
                    } else if arg == "px" {
                        let num = get_bulk_string_arg(v.next())?;
                        tx.send(Expiry {
                            key: key.clone(),
                            millis: num.parse().map_err(|_| RespError::IncorrectParams)?,
                        })
                        .await
                        .unwrap();
                    }
                }
                self.storage
                    .lock()
                    .unwrap()
                    .insert(key, Storage::new(value.into()));
                Ok(format_simple_string(String::from("OK")))
            } else if fn_name == "get" {
                let key = get_bulk_string_arg(v.next())?;
                let value = self
                    .storage
                    .lock()
                    .unwrap()
                    .get(&key)
                    .map(|storage| storage.content.clone());
                if let Some(v) = value {
                    Ok(format_bulk_string(v.into()))
                } else {
                    Ok(format_null_string())
                }
            } else if fn_name == "rpush" || fn_name == "lpush" {
                let key = get_bulk_string_arg(v.next())?;
                let mut map = self.storage.lock().unwrap();
                let storage = map
                    .entry(key)
                    .or_insert(Storage::new(StorageType::List(VecDeque::new())));
                if let StorageType::List(l) = &mut storage.content {
                    for val in v {
                        if let RespNode::BulkString(s) = val {
                            if fn_name == "rpush" {
                                self.handle_bl_pop(&mut storage.queue, s.clone());
                                l.push_back(s.into());
                            } else {
                                self.handle_bl_pop(&mut storage.queue, s.clone());
                                l.push_front(s.into());
                            }
                        }
                    }
                    let new_len = l.len();
                    return Ok(format_integer_string(new_len as isize));
                }
                panic!("shouldnt reach here")
            } else if fn_name == "lrange" {
                let list_name = get_bulk_string_arg(v.next())?;
                let start_index: isize = get_bulk_string_arg(v.next())?.parse().unwrap();
                let stop_index: isize = get_bulk_string_arg(v.next())?.parse().unwrap();
                let lock = self.storage.lock().unwrap();
                let storage = lock.get(&list_name);
                if let Some(storage) = storage
                    && let StorageType::List(v) = &storage.content
                {
                    let len = v.len();
                    let start_index = normalise_lrange_indices(len, start_index);
                    let stop_index = normalise_lrange_indices(len, stop_index);
                    if start_index >= len || start_index > stop_index {
                        Ok(format_array(EMPTY_ARRAY))
                    } else {
                        let mut bs = Vec::new();
                        for s in v.range(start_index..=stop_index) {
                            bs.push(format_bulk_string((*s).clone().into()));
                        }
                        Ok(format_array(&bs))
                    }
                } else {
                    Ok(format_array(EMPTY_ARRAY))
                }
            } else if fn_name == "llen" {
                let list_name = get_bulk_string_arg(v.next())?;
                let map = self.storage.lock().unwrap();
                let storage = map.get(&list_name);
                if let Some(storage) = storage
                    && let StorageType::List(l) = &storage.content
                {
                    return Ok(format_integer_string(l.len().cast_signed()));
                }
                Ok(format_integer_string(0))
            } else if fn_name == "lpop" {
                let list_name = get_bulk_string_arg(v.next())?;
                let pop_num: usize = get_bulk_string_arg_optional(v.next())
                    .map(|p| p.parse().unwrap())
                    .unwrap_or(1);
                let mut map = self.storage.lock().unwrap();
                let storage = map.get_mut(&list_name);
                if let Some(storage) = storage
                    && let StorageType::List(l) = &mut storage.content
                {
                    let pop_num = if pop_num >= l.len() { l.len() } else { pop_num };
                    let mut items = Vec::new();
                    if pop_num == 0 {
                        return Ok(format_null_string());
                    }
                    for _ in 0..pop_num {
                        items.push(format_bulk_string(l.pop_front().unwrap().into()));
                    }
                    if pop_num == 1 {
                        return Ok(items.into_iter().next().unwrap());
                    }
                    return Ok(format_array(&items));
                }
                Ok(format_null_string())
            } else if fn_name == "blpop" {
                let list_name = get_bulk_string_arg(v.next())?;
                let timeout = get_bulk_string_arg(v.next())?.parse::<f32>().unwrap() * 1000.0;

                let front_pop = self.blpop_front(&list_name);
                match front_pop {
                    Some(el) => Ok(format_array(&[
                        format_bulk_string(list_name),
                        format_bulk_string(el.into()),
                    ])),
                    None => {
                        let (tx, rx) = oneshot::channel();
                        self.storage
                            .lock()
                            .unwrap()
                            .get_mut(&list_name)
                            .unwrap()
                            .queue
                            .get_or_insert(tx);

                        if timeout == 0.0 {
                            let value = rx.await.unwrap();
                            Ok(format_array(&[
                                format_bulk_string(list_name),
                                format_bulk_string(value.into()),
                            ]))
                        } else if timeout > 0.0 {
                            tokio::select! {
                                _ = tokio::time::sleep(Duration::from_millis(timeout as u64)) => {
                                            self.storage
                                .lock()
                                .unwrap()
                                .get_mut(&list_name)
                                .unwrap()
                                .queue = None;
                            Ok(format_null_array())


                                }
                                 msg = rx => {
                                    let value = msg.unwrap();
                                    Ok(format_array(&[
                                format_bulk_string(list_name),
                                format_bulk_string(value.into()),
                            ]))


                                }
                            }
                        } else {
                            panic!("negative timeout")
                        }
                    }
                }
            } else if fn_name == "type" {
                let key = get_bulk_string_arg(v.next())?;
                match self.storage.lock().unwrap().get(&key) {
                    Some(v) => Ok(format_simple_string(v.content.to_string())),
                    None => Ok(format_simple_string(String::from("none"))),
                }
            } else if fn_name == "xadd" {
                let stream_key = get_bulk_string_arg(v.next())?;
                let mut id = get_bulk_string_arg(v.next())?;
                match self.validate_stream_id(&stream_key, id) {
                    Ok(processed_id) => id = processed_id,
                    Err(s) => return Ok(format_error_string(&s)),
                }
                let mut kp_vec = Vec::new();
                while let Ok(key) = get_bulk_string_arg(v.next()) {
                    let value = get_bulk_string_arg(v.next())?;
                    kp_vec.push((key, value));
                }
                let mut lock = self.storage.lock().unwrap();
                let stream = lock
                    .entry(stream_key)
                    .or_insert(Storage::new(StorageType::Stream(Vec::new())));
                if let StorageType::Stream(v) = &mut stream.content {
                    v.push(StreamEntries {
                        id: id.clone(),
                        data: kp_vec,
                    });
                }
                Ok(format_bulk_string(id))
            } else if fn_name == "xrange" {
                let stream_key = get_bulk_string_arg(v.next())?;
                let mut start_index = get_bulk_string_arg(v.next())?;
                let mut stop_index = get_bulk_string_arg(v.next())?;
                let lock = self.storage.lock().unwrap();
                let storage = lock.get(&stream_key);
                if let Some(storage) = storage
                    && let StorageType::Stream(v) = &storage.content
                {
                    let start_index = if start_index == "-" {
                        v.first().unwrap().id.clone()
                    } else if start_index.contains('-') {
                        start_index
                    } else {
                        start_index.push_str("-0");
                        start_index
                    };
                    let stop_index = if stop_index.contains('-') {
                        stop_index
                    } else {
                        let milli_index = v
                            .binary_search_by_key(&stop_index.as_str(), |entry| {
                                entry.id.split_once('-').unwrap().0
                            })
                            .unwrap();
                        let mut max_seq = 0;
                        for i in &v[milli_index..] {
                            let (curr_milli, seq) = i.id.split_once('-').unwrap();
                            let seq = seq.parse().unwrap();
                            if curr_milli == stop_index && seq >= max_seq {
                                max_seq = seq;
                            } else {
                                break;
                            }
                        }
                        stop_index.push_str(&format!("-{}", max_seq));
                        stop_index
                    };
                    let start_index = v
                        .binary_search_by_key(&start_index.as_str(), |entry| entry.id.as_str())
                        .unwrap();
                    let stop_index = v
                        .binary_search_by_key(&stop_index.as_str(), |entry| entry.id.as_str())
                        .unwrap();
                    let mut result_strings = Vec::new();
                    for e in &v[start_index..=stop_index] {
                        let mut entry_vec = Vec::new();
                        entry_vec.push(format_bulk_string(e.id.to_string()));
                        let mut kp_vec = Vec::new();
                        for (key, value) in e.data.iter() {
                            kp_vec.push(format_bulk_string(key.clone()));
                            kp_vec.push(format_bulk_string(value.clone()));
                        }
                        entry_vec.push(format_array(&kp_vec));
                        result_strings.push(format_array(&entry_vec));
                    }
                    return Ok(format_array(&result_strings));
                }
                Ok(format_array(EMPTY_ARRAY))
            } else {
                panic!("unsupported fn name")
            }
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
