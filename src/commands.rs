use std::{collections::HashMap, sync::LazyLock, vec::IntoIter};

use futures::future::BoxFuture;
use tokio::sync::mpsc::Sender;

use crate::{
    CommandRequest, CommandResponse, Expiry, RespServer,
    resp::{error::RespError, parser::RespNode},
};

mod blpop;
mod command;
mod config;
mod discard;
mod echo;
mod exec;
mod get;
mod incr;
mod info;
mod llen;
mod lpop;
mod lpush;
mod lrange;
mod multi;
mod ping;
mod psync;
mod replconf;
mod rpush;
mod set;
mod r#type;
mod unwatch;
mod wait;
mod watch;
mod xadd;
mod xrange;
mod xread;

#[derive(Debug)]
pub struct CommandFnArgs {
    pub args: IntoIter<RespNode>,
    pub cmd_string: String,
}

impl From<CommandRequest> for CommandFnArgs {
    fn from(value: CommandRequest) -> Self {
        if let RespNode::Array(v) = value.node {
            return Self {
                args: v.into_iter(),
                cmd_string: value.command_string,
            };
        }
        panic!("Command request must be an array node")
    }
}

type CommandFn = for<'a> fn(
    &'a mut RespServer,
    CommandFnArgs,
) -> BoxFuture<'a, Result<CommandResponse, RespError>>;

pub(crate) static COMMANDS: LazyLock<HashMap<&'static str, CommandFn>> = LazyLock::new(|| {
    let commands: [(&'static str, CommandFn); 26] = [
        ("ping", |server, _args| Box::pin(server.ping())),
        ("echo", |server, args| Box::pin(server.echo(args))),
        ("set", |server, args| Box::pin(server.set(args))),
        ("get", |server, args| Box::pin(server.get(args))),
        ("rpush", |server, args| Box::pin(server.rpush(args))),
        ("lpush", |server, args| Box::pin(server.lpush(args))),
        ("lrange", |server, args| Box::pin(server.lrange(args))),
        ("llen", |server, args| Box::pin(server.llen(args))),
        ("lpop", |server, args| Box::pin(server.lpop(args))),
        ("blpop", |server, args| Box::pin(server.blpop(args))),
        ("type", |server, args| Box::pin(server.r#type(args))),
        ("xadd", |server, args| Box::pin(server.xadd(args))),
        ("xrange", |server, args| Box::pin(server.xrange(args))),
        ("xread", |server, args| Box::pin(server.xread(args))),
        ("incr", |server, args| Box::pin(server.incr(args))),
        ("multi", |server, _args| Box::pin(server.multi())),
        ("exec", |server, _args| Box::pin(server.exec())),
        ("discard", |server, _args| Box::pin(server.discard())),
        ("watch", |server, args| Box::pin(server.watch(args))),
        ("unwatch", |server, _args| Box::pin(server.unwatch())),
        ("info", |server, args| Box::pin(server.info(args))),
        ("replconf", |server, args| Box::pin(server.replconf(args))),
        ("psync", |server, args| Box::pin(server.psync(args))),
        ("wait", |server, args| Box::pin(server.wait(args))),
        ("command", |server, args| Box::pin(server.command(args))),
        ("config", |server, args| Box::pin(server.config(args))),
    ];
    HashMap::from(commands)
});
