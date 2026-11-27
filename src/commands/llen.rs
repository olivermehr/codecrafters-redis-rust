use super::{RespError, RespServer};
use crate::CommandResponse;
use crate::commands::CommandFnArgs;
use crate::{StorageType, format_integer_string, get_bulk_string_arg};

impl RespServer {
    pub(crate) async fn llen(
        &mut self,
        CommandFnArgs {
            mut args,
            cmd_string,
        }: CommandFnArgs,
    ) -> Result<CommandResponse, RespError> {
        let list_name = get_bulk_string_arg(args.next())?;
        let map = self.storage.lock().unwrap();
        let storage = map.get(&list_name);
        if let Some(storage) = storage
            && let StorageType::List(l) = &storage.content
        {
            return Ok(CommandResponse::Single(format_integer_string(
                l.len().cast_signed(),
            )));
        }
        Ok(CommandResponse::Single(format_integer_string(0)))
    }
}
