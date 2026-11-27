use super::{RespError, RespServer};
use crate::CommandResponse;
use crate::commands::CommandFnArgs;
use crate::{
    StorageType, format_array, format_bulk_string, format_null_string, get_bulk_string_arg,
    get_bulk_string_arg_optional,
};

impl RespServer {
    pub(crate) async fn lpop(
        &mut self,
        CommandFnArgs {
            mut args,
            cmd_string,
        }: CommandFnArgs,
    ) -> Result<CommandResponse, RespError> {
        let list_name = get_bulk_string_arg(args.next())?;
        let pop_num: usize = get_bulk_string_arg_optional(args.next())
            .map(|p| p.parse().unwrap())
            .unwrap_or(1);
        self.replicate_command(cmd_string).await;
        let mut map = self.storage.lock().unwrap();
        let storage = map.get_mut(&list_name);
        if let Some(storage) = storage
            && let StorageType::List(l) = &mut storage.content
        {
            let pop_num = if pop_num >= l.len() { l.len() } else { pop_num };
            let mut items = Vec::new();
            if pop_num == 0 {
                return Ok(CommandResponse::Single(format_null_string()));
            }
            for _ in 0..pop_num {
                items.push(format_bulk_string(l.pop_front().unwrap().into()));
            }
            if pop_num == 1 {
                return Ok(CommandResponse::Single(items.into_iter().next().unwrap()));
            }
            return Ok(CommandResponse::Single(format_array(&items)));
        }
        Ok(CommandResponse::Single(format_null_string()))
    }
}
