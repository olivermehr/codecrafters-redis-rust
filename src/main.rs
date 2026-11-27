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
    hash::Hash,
    net::TcpStream,
    ops::Add,
    sync::{
        Arc, Mutex, MutexGuard,
        atomic::{self, AtomicU64, AtomicUsize},
    },
    time::Duration,
};

use chrono::{DateTime, Utc};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpSocket, tcp::OwnedWriteHalf},
    sync::{
        Notify, RwLock,
        mpsc::{self, Receiver, Sender},
        oneshot, watch,
    },
};

use crate::{
    ServerRole::Master,
    commands::CommandFnArgs,
    resp::{
        error::RespError,
        parser::{
            RespNode::{self, RdbFile},
            RespParser,
        },
    },
};
mod commands;
mod resp;

#[derive(Debug, Clone)]
struct Replica {
    propagation_tx: Sender<Result<CommandResponse, RespError>>,
    last_offset: Arc<atomic::AtomicUsize>,
    ack_notifier: Arc<Notify>,
}

#[derive(Debug, Clone)]
enum ReplicaState {
    InitHandshake,
    ProcessRdb,
    Ready,
}

#[derive(Debug, Clone)]
enum ServerRole {
    Master {
        replid: String,
        master_offset: Arc<atomic::AtomicUsize>,
        connected_slaves: Arc<atomic::AtomicUsize>,
        replicas: Arc<Mutex<HashMap<usize, Replica>>>,
        ack_notifier: Arc<tokio::sync::Notify>,
    },
    Slave {
        master_host: String,
        master_port: String,
        offset: Arc<AtomicUsize>,
        state: Arc<ReplicaState>,
    },
}

impl Display for ServerRole {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Master {
                replid,
                master_offset: offset,
                connected_slaves,
                replicas,
                ack_notifier,
            } => write!(
                f,
                "role:master\nmaster_replid:{replid}\nmaster_repl_offset:{}\n",
                offset.load(atomic::Ordering::Relaxed)
            ),
            Self::Slave {
                master_host,
                master_port,
                offset,
                state,
            } => write!(f, "role:slave"),
        }
    }
}

// Server impl as the foundation of each connection
#[derive(Debug)]
struct RespServer {
    role: ServerRole,
    connection_no: usize,
    command_queue: Arc<Mutex<HashMap<usize, Option<Vec<CommandRequest>>>>>,
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
struct CommandRequest {
    node: RespNode,
    command_string: String,
}

#[derive(Debug, Clone)]
pub enum CommandResponse {
    Single(String),
    Bytes(Vec<u8>),
    Multi(Vec<CommandResponse>),
    PromoteReplicaConn,
    ReplicaResponse(Box<CommandResponse>),
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
            watched_keys: Arc::new(Mutex::new(HashMap::new())),
            connection_no: SERVER_INDEX.fetch_add(1, atomic::Ordering::Relaxed) as usize,
        }
    }

    fn get_replica_state(&self) -> Option<&ReplicaState> {
        if let ServerRole::Slave {
            master_host,
            master_port,
            offset,
            state,
        } = &self.role
        {
            return Some(state);
        }
        None
    }

    fn replicate_command(&self, cmd_string: String) {
        if let ServerRole::Master {
            replid,
            master_offset,
            connected_slaves,
            replicas,
            ack_notifier,
        } = &self.role
        {
            master_offset.fetch_add(cmd_string.len(), atomic::Ordering::Relaxed);
            let replica_lock = replicas.lock().unwrap();
            for r in replica_lock.values() {
                r.propagation_tx
                    .send(Ok(CommandResponse::Single(cmd_string.clone())));
            }
        }
    }

    fn process_incoming_tcp_stream(
        &mut self,
        buf: &mut [u8; 1024],
        read_index: &mut usize,
        bytes_read: usize,
    ) -> Vec<CommandRequest> {
        let mut parser = RespParser::new(&buf[..*read_index + bytes_read]);
        let replica_handshake = if let ServerRole::Slave {
            master_host,
            master_port,
            offset,
            state,
        } = &self.role
            && let ReplicaState::Ready = state.as_ref()
        {
            false
        } else {
            true
        };
        let (bytes_used, tree) = parser.create_tree(replica_handshake);
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
    async fn visit(&mut self, command: CommandRequest) -> Result<CommandResponse, RespError> {
        if let RespNode::Array(v) = &command.node
            && let Some(Some(command_vec)) = &mut self
                .command_queue
                .lock()
                .unwrap()
                .get_mut(&self.connection_no)
            && let Some(RespNode::BulkString(s)) = v.first()
            && !NON_MULTI_COMMANDS.contains(&s.as_str())
        {
            command_vec.push(command);
            return Ok(CommandResponse::Single(format_simple_string(String::from(
                "QUEUED",
            ))));
        }

        let mut cmd_fn_args: CommandFnArgs = command.into();
        println!("{}", cmd_fn_args);
        let fn_name = get_bulk_string_arg(cmd_fn_args.args.next())?;
        let command_fn = commands::COMMANDS
            .get(fn_name.as_str())
            .expect("unsupported fn name");
        command_fn(self, cmd_fn_args).await
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

#[tokio::main]
async fn main() {
    let cli = CommandLine::parse();

    // parse port to run server on
    let listener = if let Some(port) = cli.port {
        TcpListener::bind(format!("127.0.0.1:{}", port))
            .await
            .unwrap()
    } else {
        TcpListener::bind("127.0.0.1:6379").await.unwrap()
    };

    // get server role
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
            offset: Arc::new(atomic::AtomicUsize::new(0)),
            state: Arc::new(ReplicaState::InitHandshake),
        }
    } else {
        let replid = String::from_utf8(rng().sample_iter(Alphanumeric).take(40).collect()).unwrap();
        ServerRole::Master {
            replid,
            master_offset: Arc::new(atomic::AtomicUsize::new(0)),
            connected_slaves: Arc::new(atomic::AtomicUsize::new(0)),
            replicas: Arc::new(Mutex::new(HashMap::new())),
            ack_notifier: Arc::new(Notify::new()),
        }
    };

    let server = RespServer::new(role);

    if let ServerRole::Slave {
        master_host,
        master_port,
        offset,
        state,
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
                let stream = socket
                    .connect(format!("{master_host}:{master_port}").parse().unwrap())
                    .await
                    .unwrap();
                let (mut stream_read, mut stream_write) = stream.into_split();
                stream_write
                    .write_all(&replication_init.pop_front().unwrap().into_bytes())
                    .await
                    .unwrap();
                let mut buf = [0; 1024];
                let mut read_index = 0;

                while let Ok(bytes) = stream_read.read(&mut buf).await
                    && bytes > 0
                {
                    let tree =
                        cloned_server.process_incoming_tcp_stream(&mut buf, &mut read_index, bytes);

                    if let Some(ReplicaState::Ready) = cloned_server.get_replica_state() {
                        // ADD incoming bytes to replica offset
                        offset.fetch_add(bytes, atomic::Ordering::Relaxed);
                        for cmd_req in tree {
                            if let CommandResponse::ReplicaResponse(inner) =
                                cloned_server.visit(cmd_req).await.unwrap()
                            {
                                send_command_responses(&mut stream_write, *inner).await;
                            }
                        }
                    } else {
                        for CommandRequest {
                            node,
                            command_string,
                        } in tree
                        {
                            if let RespNode::SimpleString(s) = node {
                                let s = s.to_lowercase();
                                if s.eq_ignore_ascii_case("ok")
                                    || s.eq_ignore_ascii_case("pong")
                                        && !replication_init.is_empty()
                                {
                                    stream_write
                                        .write_all(
                                            &replication_init.pop_front().unwrap().into_bytes(),
                                        )
                                        .await;
                                }
                            } else if let RdbFile(v) = node
                                && let ServerRole::Slave {
                                    master_host,
                                    master_port,
                                    offset,
                                    state,
                                } = &mut cloned_server.role
                            {
                                *state = ReplicaState::Ready.into()
                            }
                        }
                    }
                }
            }
        });
    }

    loop {
        match listener.accept().await {
            Ok((stream, _)) => {
                let mut cloned_server = server.clone_for_conn();

                // connection task
                tokio::spawn(async move {
                    let mut buf = [0; 1024];
                    let mut read_index = 0;
                    let (tx, rx) = watch::channel(());
                    let (command_tx, mut command_rx) = mpsc::channel(100);
                    let (mut stream_read, mut stream_write) = stream.into_split();
                    let (tcp_channel_tx, mut tcp_channel_rx) = mpsc::channel(100);

                    // task for sending command responses over tcp
                    tokio::spawn({
                        let send_task_clone = cloned_server.clone_for_comm();
                        let tcp_channel_tx_clone = tcp_channel_tx.clone();
                        async move {
                            while let Some(res) = tcp_channel_rx.recv().await {
                                match res {
                                    Ok(response) => {
                                        if send_command_responses(&mut stream_write, response).await
                                            && let ServerRole::Master {
                                                replid,
                                                master_offset,
                                                connected_slaves,
                                                replicas,
                                                ack_notifier,
                                            } = &send_task_clone.role
                                        {
                                            replicas.lock().unwrap().insert(
                                                send_task_clone.connection_no,
                                                Replica {
                                                    propagation_tx: tcp_channel_tx_clone.clone(),
                                                    last_offset: Arc::new(
                                                        atomic::AtomicUsize::new(0),
                                                    ),

                                                    ack_notifier: ack_notifier.clone(),
                                                },
                                            );
                                        }
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
                        }
                    });

                    // task for processing commands coming from connection
                    tokio::spawn({
                        let mut command_clone_server = cloned_server.clone_for_comm();
                        let mut kill_switch = rx.clone();
                        let tcp_channel_tx_clone = tcp_channel_tx.clone();

                        async move {
                            while let Some(cmd_req) = command_rx.recv().await {
                                tokio::select! {
                                    res = command_clone_server.visit(cmd_req) =>  {
                                        tcp_channel_tx_clone.send(res).await;
                                        }
                                    _ = kill_switch.changed() => {break}
                                }
                            }
                        }
                    });
                    while let Ok(bytes) = stream_read.read(&mut buf[read_index..]).await
                        && bytes > 0
                    {
                        let tree = cloned_server.process_incoming_tcp_stream(
                            &mut buf,
                            &mut read_index,
                            bytes,
                        );
                        for command in tree {
                            command_tx.send(command).await;
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
        CommandResponse::ReplicaResponse(r) => {
            Box::pin(send_command_responses(stream_writer, *r)).await
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
