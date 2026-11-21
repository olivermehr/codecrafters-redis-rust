use crate::CommandResponse;
use std::collections::HashSet;

use crate::{
    RespServer, Storage, StorageType, commands::CommandArgs, format_array, format_error_string,
    format_integer_string, format_simple_string, get_bulk_string_arg, resp::error::RespError,
};

impl RespServer {
    pub async fn replconf(&mut self, mut v: CommandArgs) -> Result<CommandResponse, RespError> {
        Ok(CommandResponse::Single(format_simple_string(String::from("OK"))))
    }
}
