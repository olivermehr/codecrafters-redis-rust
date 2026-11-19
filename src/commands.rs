use std::{collections::HashMap, sync::LazyLock, vec::IntoIter};

use futures::future::BoxFuture;
use tokio::sync::mpsc::Sender;

use crate::{
    Expiry, RespServer,
    resp::{error::RespError, parser::RespNode},
};

mod blpop;
mod echo;
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
type CommandFn = for<'a> fn(
    &'a mut RespServer,
    CommandArgs,
    Sender<Expiry>,
) -> BoxFuture<'a, Result<String, RespError>>;

pub(crate) static COMMANDS: LazyLock<HashMap<&'static str, CommandFn>> = LazyLock::new(|| {
    let commands: [(&'static str, CommandFn); 16] = [
        ("ping", |server, _args, _tx| Box::pin(server.ping())),
        ("echo", |server, args, _tx| Box::pin(server.echo(args))),
        ("set", |server, args, tx| Box::pin(server.set(args, tx))),
        ("get", |server, args, _tx| Box::pin(server.get(args))),
        ("rpush", |server, args, _tx| Box::pin(server.rpush(args))),
        ("lpush", |server, args, _tx| Box::pin(server.lpush(args))),
        ("lrange", |server, args, _tx| Box::pin(server.lrange(args))),
        ("llen", |server, args, _tx| Box::pin(server.llen(args))),
        ("lpop", |server, args, _tx| Box::pin(server.lpop(args))),
        ("blpop", |server, args, _tx| Box::pin(server.blpop(args))),
        ("type", |server, args, _tx| Box::pin(server.r#type(args))),
        ("xadd", |server, args, _tx| Box::pin(server.xadd(args))),
        ("xrange", |server, args, _tx| Box::pin(server.xrange(args))),
        ("xread", |server, args, _tx| Box::pin(server.xread(args))),
        ("incr", |server, args, tx| Box::pin(server.incr(args))),
        ("multi", |server, args, tx| Box::pin(server.multi(args))),
    ];
    HashMap::from(commands)
});
