use crate::CommandResponse;
use crate::{
    RespServer, Storage, StorageType, commands::CommandArgs, format_error_string,
    format_integer_string, format_simple_string, get_bulk_string_arg, resp::error::RespError,
};

impl RespServer {
    pub async fn multi(&mut self) -> Result<CommandResponse, RespError> {
        self.command_queue
            .lock()
            .unwrap()
            .insert(self.connection_no, Some(Vec::new()));
        Ok(CommandResponse::Single(format_simple_string(String::from("OK"))))
    }
}
