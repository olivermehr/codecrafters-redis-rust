use std::{collections::HashMap, sync::LazyLock, vec::IntoIter};

use futures::future::BoxFuture;
use tokio::sync::mpsc::Sender;

use crate::{
    CommandResponse, Expiry, RespServer,
    resp::{error::RespError, parser::RespNode},
};

mod blpop;
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

type CommandArgs = IntoIter<RespNode>;
type CommandFn = for<'a> fn(
    &'a mut RespServer,
    CommandArgs,
) -> BoxFuture<'a, Result<CommandResponse, RespError>>;

pub(crate) static COMMANDS: LazyLock<HashMap<&'static str, CommandFn>> = LazyLock::new(|| {
    let commands: [(&'static str, CommandFn); 24] = [
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
        ("multi", |server, args| Box::pin(server.multi())),
        ("exec", |server, args| Box::pin(server.exec())),
        ("discard", |server, args| Box::pin(server.discard())),
        ("watch", |server, args| Box::pin(server.watch(args))),
        ("unwatch", |server, args| Box::pin(server.unwatch())),
        ("info", |server, args| Box::pin(server.info(args))),
        ("replconf", |server, args| Box::pin(server.replconf(args))),
        ("psync", |server, args| Box::pin(server.psync(args))),
        ("wait", |server, args| Box::pin(server.wait(args))),
    ];
    HashMap::from(commands)
});
