use crate::CommandResponse;
use super::{CommandArgs, RespError, RespServer};
use crate::{StorageType, format_integer_string, get_bulk_string_arg};

impl RespServer {
    pub(crate) async fn llen(&mut self, mut v: CommandArgs) -> Result<CommandResponse, RespError> {
        let list_name = get_bulk_string_arg(v.next())?;
        let map = self.storage.lock().unwrap();
        let storage = map.get(&list_name);
        if let Some(storage) = storage
            && let StorageType::List(l) = &storage.content
        {
            return Ok(CommandResponse::Single(format_integer_string(l.len().cast_signed())));
        }
        Ok(CommandResponse::Single(format_integer_string(0)))
    }
}
