use super::{CommandArgs, RespError, RespNode, RespServer};
use crate::{Storage, StorageType, format_integer_string, get_bulk_string_arg};
use std::collections::VecDeque;

impl RespServer {
    pub(crate) async fn rpush(&mut self, mut v: CommandArgs) -> Result<String, RespError> {
        let key = get_bulk_string_arg(v.next())?;
        let mut map = self.storage.lock().unwrap();
        let storage = map
            .entry(key)
            .or_insert(Storage::new(StorageType::List(VecDeque::new())));
        if let StorageType::List(l) = &mut storage.content {
            for val in v {
                if let RespNode::BulkString(s) = val {
                    self.handle_bl_pop(&mut storage.queue, s.clone());
                    l.push_back(s.into());
                }
            }
            let new_len = l.len();
            return Ok(format_integer_string(new_len as isize));
        }
        panic!("shouldnt reach here")
    }
}
