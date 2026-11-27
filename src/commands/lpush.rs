use super::{RespError, RespNode, RespServer};
use crate::CommandResponse;
use crate::commands::CommandFnArgs;
use crate::{Storage, StorageType, format_integer_string, get_bulk_string_arg};
use std::collections::VecDeque;

impl RespServer {
    pub(crate) async fn lpush(
        &mut self,
        CommandFnArgs {
            mut args,
            cmd_string,
        }: CommandFnArgs,
    ) -> Result<CommandResponse, RespError> {
        let key = get_bulk_string_arg(args.next())?;
        self.replicate_command(cmd_string).await;
        let mut map = self.storage.lock().unwrap();
        let storage = map
            .entry(key)
            .or_insert(Storage::new(StorageType::List(VecDeque::new())));
        if let StorageType::List(l) = &mut storage.content {
            for val in args {
                if let RespNode::BulkString(s) = val {
                    self.handle_bl_pop(&mut storage.queue, s.clone());
                    l.push_front(s.into());
                }
            }
            let new_len = l.len();
            return Ok(CommandResponse::Single(format_integer_string(
                new_len as isize,
            )));
        }
        panic!("shouldnt reach here")
    }
}
