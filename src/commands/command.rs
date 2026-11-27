use crate::{CommandResponse, commands::CommandFnArgs};
use std::collections::HashSet;

use crate::{
    RespServer, Storage, StorageType, format_array, format_error_string, format_integer_string,
    format_simple_string, get_bulk_string_arg, resp::error::RespError,
};

impl RespServer {
    pub async fn command(
        &mut self,
        CommandFnArgs {
            mut args,
            cmd_string,
        }: CommandFnArgs,
    ) -> Result<CommandResponse, RespError> {
        Ok(CommandResponse::Single(format_simple_string(String::from(
            "OK",
        ))))
    }
}

