use crate::{
    RespServer, Storage, StorageType, commands::CommandArgs, format_error_string,
    format_integer_string, format_simple_string, get_bulk_string_arg, resp::error::RespError,
};

impl RespServer {
    pub async fn exec(&mut self, mut v: CommandArgs) -> Result<String, RespError> {
        match &self.command_queue {
            Some(commands) => {
                todo!()
            }
            None => Ok(format_error_string("EXEC without MULTI")),
        }
    }
}
