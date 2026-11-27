use super::{RespError, RespServer};
use crate::CommandResponse;
use crate::commands::CommandFnArgs;
use crate::{format_bulk_string, format_null_string, get_bulk_string_arg};

impl RespServer {
    pub(crate) async fn get(
        &mut self,
        CommandFnArgs {
            mut args,
            cmd_string: _,
        }: CommandFnArgs,
    ) -> Result<CommandResponse, RespError> {
        let key = get_bulk_string_arg(args.next())?;
        let value = self
            .storage
            .lock()
            .unwrap()
            .get(&key)
            .map(|storage| storage.content.clone());
        if let Some(v) = value {
            Ok(CommandResponse::Single(format_bulk_string(v.into())))
        } else {
            Ok(CommandResponse::Single(format_null_string()))
        }
    }
}
