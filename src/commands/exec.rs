use crate::{
    RespServer, Storage, StorageType, commands::CommandArgs, format_array, format_error_string,
    format_integer_string, format_simple_string, get_bulk_string_arg, resp::error::RespError,
};

impl RespServer {
    pub async fn exec(&mut self, mut v: CommandArgs) -> Result<String, RespError> {
        match self.command_queue.take() {
            Some(commands) => {
                if commands.is_empty() {
                    return Ok(format_array(&[]));
                }
                let mut response_vec = Vec::new();
                for node in commands {
                    response_vec.push(self.visit(node).await?)
                }
                Ok(format_array(&response_vec))
            }
            None => Ok(format_error_string("EXEC without MULTI")),
        }
    }
}
