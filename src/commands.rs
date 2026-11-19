use std::{collections::HashMap, sync::LazyLock, vec::IntoIter};

use futures::future::BoxFuture;
use tokio::sync::mpsc::Sender;

use crate::{
    Expiry, RespServer,
    resp::{error::RespError, parser::RespNode},
};

mod blpop;
mod discard;
mod echo;
mod exec;
mod get;
mod incr;
mod llen;
mod lpop;
mod lpush;
mod lrange;
mod multi;
mod ping;
mod rpush;
mod set;
mod r#type;
mod xadd;
mod xrange;
mod xread;

type CommandArgs = IntoIter<RespNode>;
type CommandFn =
    for<'a> fn(&'a mut RespServer, CommandArgs) -> BoxFuture<'a, Result<String, RespError>>;

pub(crate) static COMMANDS: LazyLock<HashMap<&'static str, CommandFn>> = LazyLock::new(|| {
    let commands: [(&'static str, CommandFn); 18] = [
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
        ("multi", |server, args| Box::pin(server.multi(args))),
        ("exec", |server, args| Box::pin(server.exec(args))),
        ("discard", |server, args| Box::pin(server.discard(args))),
    ];
    HashMap::from(commands)
});
