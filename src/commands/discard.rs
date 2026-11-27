use crate::CommandResponse;
use crate::{
    RespServer, Storage, StorageType, format_array, format_error_string, format_integer_string,
    format_simple_string, get_bulk_string_arg, resp::error::RespError,
};

impl RespServer {
    pub async fn discard(&mut self) -> Result<CommandResponse, RespError> {
        if let Some(queue) = self
            .command_queue
            .lock()
            .unwrap()
            .get_mut(&self.connection_no)
            && let Some(_c) = queue.take()
        {
            if let Some(wk) = self
                .watched_keys
                .lock()
                .unwrap()
                .get_mut(&self.connection_no)
            {
                wk.clear();
            }
            return Ok(CommandResponse::Single(format_simple_string(String::from(
                "OK",
            ))));
        }
        Ok(CommandResponse::Single(format_error_string(
            "DISCARD without MULTI",
        )))
    }
}
