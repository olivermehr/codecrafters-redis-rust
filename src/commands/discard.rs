use crate::{
    RespServer, Storage, StorageType, commands::CommandArgs, format_array, format_error_string,
    format_integer_string, format_simple_string, get_bulk_string_arg, resp::error::RespError,
};

impl RespServer {
    pub async fn discard(&mut self, mut v: CommandArgs) -> Result<String, RespError> {
        match self.command_queue.take() {
            Some(_) => Ok(format_simple_string(String::from("OK"))),
            None => Ok(format_error_string("DISCARD without MULTI")),
        }
    }
}
