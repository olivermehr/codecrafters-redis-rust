#![allow(unused_imports)]
use clap::Parser;
use core::error;
use rand::{
    RngExt,
    distr::{Alphanumeric, StandardUniform},
    rng,
};
use std::{
    array,
    borrow::Cow,
    collections::{HashMap, HashSet, VecDeque, btree_map::Entry},
    fmt::Display,
    fs::read,
    net::TcpStream,
    ops::Add,
    sync::{
        Arc, Mutex, MutexGuard,
        atomic::{self, AtomicU64},
    },
    time::Duration,
};

use chrono::{DateTime, Utc};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpSocket, tcp::OwnedWriteHalf},
    sync::{
        RwLock,
        mpsc::{self, Receiver, Sender},
        oneshot, watch,
    },
};

use crate::resp::{
    error::RespError,
    parser::{RespNode, RespParser},
};
mod commands;
mod resp;

#[derive(Debug, Clone)]
enum ServerRole {
    Master {
        replid: String,
        offset: usize,
    },
    Slave {
        master_host: String,
        master_port: String,
    },
}

impl Display for ServerRole {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Master { replid, offset } => write!(
                f,
                "role:master\nmaster_replid:{replid}\nmaster_repl_offset:{offset}\n"
            ),
            Self::Slave {
                master_host,
                master_port,
            } => write!(f, "role:slave"),
        }
    }
}

// Server impl as the foundation of each connection
#[derive(Debug)]
struct RespServer {
    role: ServerRole,
    connection_no: usize,
    replica_conns: Arc<RwLock<Vec<Arc<tokio::sync::Mutex<OwnedWriteHalf>>>>>,
    command_queue: Arc<Mutex<HashMap<usize, Option<Vec<RespNode>>>>>,
    watched_keys: Arc<Mutex<HashMap<usize, Vec<(String, bool)>>>>,
    expiry_tx: Sender<Expiry>,
    storage: Arc<Mutex<HashMap<String, Storage>>>,
}

impl Clone for RespServer {
    fn clone(&self) -> Self {
        let new_conn_no = SERVER_INDEX.fetch_add(1, atomic::Ordering::Relaxed) as usize;
        println!("New connection no for clone {}", new_conn_no);
        Self {
            role: self.role.clone(),
            connection_no: new_conn_no,
            command_queue: self.command_queue.clone(),
            watched_keys: self.watched_keys.clone(),
            expiry_tx: self.expiry_tx.clone(),
            replica_conns: self.replica_conns.clone(),
            storage: self.storage.clone(),
        }
    }
}

impl Drop for RespServer {
    fn drop(&mut self) {
        println!("Dropping connection {}", self.connection_no);
        SERVER_INDEX.fetch_sub(1, atomic::Ordering::Relaxed);
    }
}

#[derive(Debug)]
struct Storage {
    content: StorageType,
    watched: HashSet<usize>,
    queue: VecDeque<tokio::sync::oneshot::Sender<StorageType>>,
}

impl Storage {
    fn new(content: StorageType) -> Self {
        Self {
            content,
            queue: VecDeque::new(),
            watched: HashSet::new(),
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

#[derive(Debug, Clone)]
pub enum CommandResponse {
    Single(String),
    Bytes(Vec<u8>),
    Multi(Vec<CommandResponse>),
    PromoteReplicaConn,
}

static EMPTY_ARRAY: &[String; 0] = &[];
static NON_MULTI_COMMANDS: &[&str] = &["exec", "discard", "watch", "unwatch"];
static SERVER_INDEX: atomic::AtomicU64 = AtomicU64::new(0);

impl RespServer {
    fn new(role: ServerRole) -> Self {
        let (tx, mut rx) = mpsc::channel::<Expiry>(50);
        let storage = Arc::new(Mutex::new(HashMap::new()));

        // fire up the worker that handles key/value expiry
        tokio::spawn({
            let storage_clone = storage.clone();
            async move {
                while let Some(expiry) = rx.recv().await {
                    tokio::spawn({
                        let spawn_storage = storage_clone.clone();
                        async move {
                            tokio::time::sleep(Duration::from_millis(expiry.millis)).await;
                            loop {
                                if let Ok(mut map) = spawn_storage.try_lock() {
                                    map.remove(&expiry.key);
                                    break;
                                }
                            }
                        }
                    });
                }
            }
        });
        Self {
            role,
            storage,
            command_queue: Arc::new(Mutex::new(HashMap::new())),
            expiry_tx: tx,
            replica_conns: Arc::new(RwLock::new(Vec::new())),
            watched_keys: Arc::new(Mutex::new(HashMap::new())),
            connection_no: SERVER_INDEX.fetch_add(1, atomic::Ordering::Relaxed) as usize,
        }
    }
    async fn visit(&mut self, node: RespNode) -> Result<CommandResponse, RespError> {
        if let RespNode::Array(v) = &node
            && let Some(Some(command_vec)) = &mut self
                .command_queue
                .lock()
                .unwrap()
                .get_mut(&self.connection_no)
            && let Some(RespNode::BulkString(s)) = v.first()
            && !NON_MULTI_COMMANDS.contains(&s.as_str())
        {
            command_vec.push(node);
            Ok(CommandResponse::Single(format_simple_string(String::from(
                "QUEUED",
            ))))
        } else if let RespNode::Array(v) = node {
            let mut args = v.into_iter();
            let fn_name = get_bulk_string_arg(args.next())?;
            let command = commands::COMMANDS
                .get(fn_name.as_str())
                .expect("unsupported fn name");
            command(self, args).await
        } else {
            Err(RespError::InvalidSyntax)
        }
    }

    // Clone per connection (increments conn number)
    fn clone_for_conn(&self) -> Self {
        self.clone()
    }
    // clone per command does increment conn number
    fn clone_for_comm(&self) -> Self {
        Self {
            role: self.role.clone(),
            connection_no: self.connection_no,
            command_queue: self.command_queue.clone(),
            watched_keys: self.watched_keys.clone(),
            expiry_tx: self.expiry_tx.clone(),
            storage: self.storage.clone(),
            replica_conns: self.replica_conns.clone(),
        }
    }

    fn handle_bl_pop(
        &self,
        queue: &mut VecDeque<tokio::sync::oneshot::Sender<StorageType>>,
        value: String,
    ) {
        if let Some(sender) = queue.pop_front() {
            sender.send(value.into());
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

#[derive(Debug, Parser)]
struct CommandLine {
    #[arg(short, long)]
    port: Option<usize>,
    #[arg(short, long)]
    replicaof: Option<String>,
}

fn process_incoming_tcp_stream(
    buf: &mut [u8; 1024],
    read_index: &mut usize,
    bytes_read: usize,
) -> Vec<(RespNode, String)> {
    let s = String::from_utf8_lossy(&buf[..*read_index + bytes_read])
        .into_owned()
        .to_lowercase();
    let mut parser = RespParser::new(s.chars().collect());
    let (bytes_used, tree) = parser.create_tree();
    if bytes_used >= buf.len() {
        buf.fill(0);
    } else {
        if bytes_used == 0 {
            *read_index = bytes_read;
        } else {
            *read_index = 0;
            buf.copy_within(bytes_used.., 0);
            let remaining = buf.len() - bytes_used;
            for i in buf[remaining..].iter_mut() {
                *i = 0;
            }
        }
    }
    tree
}

#[tokio::main]
async fn main() {
    let cli = CommandLine::parse();

    let listener = if let Some(port) = cli.port {
        TcpListener::bind(format!("127.0.0.1:{}", port))
            .await
            .unwrap()
    } else {
        TcpListener::bind("127.0.0.1:6379").await.unwrap()
    };
    let role = if let Some(replica) = cli.replicaof {
        let (mut host, port) = replica
            .split_once(' ')
            .expect("incorrect replica info format");
        host = if host == "localhost" {
            "127.0.0.1"
        } else {
            host
        };

        ServerRole::Slave {
            master_host: host.to_string(),
            master_port: port.to_string(),
        }
    } else {
        let replid = String::from_utf8(rng().sample_iter(Alphanumeric).take(40).collect()).unwrap();
        ServerRole::Master { replid, offset: 0 }
    };

    let mut server = RespServer::new(role);

    if let ServerRole::Slave {
        master_host,
        master_port,
    } = server.role.clone()
    {
        tokio::spawn({
            let mut cloned_server = server.clone_for_comm();
            let listening_port = listener.local_addr().as_ref().unwrap().port().to_string();
            async move {
                let mut replication_init = VecDeque::from([
                    format_array(&[format_bulk_string(String::from("PING"))]),
                    format_array(&[
                        format_bulk_string(String::from("REPLCONF")),
                        format_bulk_string(String::from("listening-port")),
                        format_bulk_string(listening_port),
                    ]),
                    format_array(&[
                        format_bulk_string(String::from("REPLCONF")),
                        format_bulk_string(String::from("capa")),
                        format_bulk_string(String::from("psync2")),
                    ]),
                    format_array(&[
                        format_bulk_string(String::from("PSYNC")),
                        format_bulk_string(String::from("?")),
                        format_bulk_string((-1).to_string()),
                    ]),
                ]);
                let socket = TcpSocket::new_v4().unwrap();
                let mut stream = socket
                    .connect(format!("{master_host}:{master_port}").parse().unwrap())
                    .await
                    .unwrap();
                stream
                    .write_all(&replication_init.pop_front().unwrap().into_bytes())
                    .await
                    .unwrap();
                let mut buf = [0; 1024];
                let mut read_index = 0;

                // replica setup loop
                'read_loop: while let Ok(bytes) = stream.read(&mut buf).await
                    && bytes > 0
                {
                    let tree = process_incoming_tcp_stream(&mut buf, &mut read_index, bytes);
                    for (node, command_string) in tree {
                        if let RespNode::SimpleString(s) = node {
                            if s == "ok" && replication_init.is_empty() {
                                break 'read_loop;
                            } else if s == "ok" || s == "pong" {
                                stream
                                    .write_all(&replication_init.pop_front().unwrap().into_bytes())
                                    .await;
                            }
                        }
                    }
                }
                while let Ok(bytes) = stream.read(&mut buf).await
                    && bytes > 0
                {
                    let tree = process_incoming_tcp_stream(&mut buf, &mut read_index, bytes);
                    for (node, command_string) in tree {
                        println!("{node:?}");
                        cloned_server.visit(node).await;
                    }
                }
            }
        });
    }

    let replicas_tx = if let ServerRole::Master {
        replid: _,
        offset: _,
    } = server.role
    {
        let (replicas_tx, mut replicas_rx) = tokio::sync::mpsc::channel::<String>(100);
        // launch replication task
        tokio::spawn({
            let cs = server.clone_for_comm();

            async move {
                while let Some(command) = replicas_rx.recv().await {
                    let replicas = cs.replica_conns.read().await;
                    for r in replicas.iter() {
                        {
                            r.lock()
                                .await
                                .write_all(&command.clone().into_bytes())
                                .await
                                .unwrap();
                        }
                    }
                }
            }
        });
        Some(replicas_tx)
    } else {
        None
    };

    loop {
        match listener.accept().await {
            Ok((stream, _)) => {
                let cloned_server = server.clone_for_conn();
                let cloned_replicas_tx = replicas_tx.clone();
                tokio::spawn(async move {
                    let mut buf = [0; 1024];
                    let mut read_index = 0;
                    let (tx, rx) = watch::channel(());
                    let (command_tx, mut command_rx) = mpsc::channel(100);
                    let (mut stream_read, mut stream_write) = stream.into_split();
                    tokio::spawn({
                        let mut command_clone_server = cloned_server.clone_for_comm();
                        let mut kill_switch = rx.clone();
                        async move {
                            while let Some(command) = command_rx.recv().await {
                                tokio::select! {
                                            res = command_clone_server.visit(command) =>  {
                                                match res {
                                                                  Ok(response) => {
                                                                    if send_command_responses(&mut stream_write,response).await {
                                                                        command_clone_server.replica_conns.write().await.push(Arc::new(tokio::sync::Mutex::new(stream_write)));
                                                                        break
                                                                    }
                                                                },
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
                                            RespError::TcpStreamDropped => {
                                                format_error_string("tcp stream dropped")
                                            }
                                        };
                                        if error != "tcp stream dropped" {
                                            stream_write.write_all(error.as_bytes()).await.unwrap()
                                        }
                                    }
                                }
                                                }
                                            _ = kill_switch.changed() => {break}
                                        }
                            }
                        }
                    });
                    while let Ok(bytes) = stream_read.read(&mut buf[read_index..]).await
                        && bytes > 0
                    {
                        let tree = process_incoming_tcp_stream(&mut buf, &mut read_index, bytes);

                        for (node, command_string) in tree.into_iter() {
                            command_tx.send(node).await;
                            if let Some(cloned_replicas_tx) = cloned_replicas_tx.as_ref() {
                                cloned_replicas_tx.send(command_string).await;
                            }
                        }
                    }
                    tx.send(())
                });
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}

async fn send_command_responses(
    stream_writer: &mut OwnedWriteHalf,
    response: CommandResponse,
) -> bool {
    //returns true when replica connection needs upgrading - kinda hacky until i figure out a better solution
    match response {
        CommandResponse::Single(s) => {
            stream_writer.write_all(s.as_bytes()).await.unwrap();
            false
        }
        CommandResponse::Multi(v) => {
            let mut ret = false;
            for res in v {
                ret = Box::pin(send_command_responses(stream_writer, res)).await;
            }
            ret
        }
        CommandResponse::Bytes(b) => {
            stream_writer.write_all(&b).await.unwrap();
            false
        }
        CommandResponse::PromoteReplicaConn => true,
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
