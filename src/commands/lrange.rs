use super::{RespError, RespServer};
use crate::CommandResponse;
use crate::commands::CommandFnArgs;
use crate::{
    EMPTY_ARRAY, StorageType, format_array, format_bulk_string, get_bulk_string_arg,
    normalise_lrange_indices,
};

impl RespServer {
    pub(crate) async fn lrange(
        &mut self,
        CommandFnArgs {
            mut args,
            cmd_string,
        }: CommandFnArgs,
    ) -> Result<CommandResponse, RespError> {
        let list_name = get_bulk_string_arg(args.next())?;
        let start_index: isize = get_bulk_string_arg(args.next())?.parse().unwrap();
        let stop_index: isize = get_bulk_string_arg(args.next())?.parse().unwrap();
        let lock = self.storage.lock().unwrap();
        let storage = lock.get(&list_name);
        if let Some(storage) = storage
            && let StorageType::List(v) = &storage.content
        {
            let len = v.len();
            let start_index = normalise_lrange_indices(len, start_index);
            let stop_index = normalise_lrange_indices(len, stop_index);
            if start_index >= len || start_index > stop_index {
                Ok(CommandResponse::Single(format_array(EMPTY_ARRAY)))
            } else {
                let mut bs = Vec::new();
                for s in v.range(start_index..=stop_index) {
                    bs.push(format_bulk_string((*s).clone().into()));
                }
                Ok(CommandResponse::Single(format_array(&bs)))
            }
        } else {
            Ok(CommandResponse::Single(format_array(EMPTY_ARRAY)))
        }
    }
}
