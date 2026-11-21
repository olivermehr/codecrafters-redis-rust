use crate::{
    RespServer, Storage, StorageType, commands::CommandArgs, format_array, format_error_string,
    format_integer_string, format_simple_string, get_bulk_string_arg, resp::error::RespError,
};

impl RespServer {
    pub async fn discard(&mut self) -> Result<String, RespError> {
        if let Some(queue) = self
            .command_queue
            .lock()
            .unwrap()
            .get_mut(&self.connection_no)
            && let Some(c) = queue.take()
        {
            if let Some(wk) = self
                .watched_keys
                .lock()
                .unwrap()
                .get_mut(&self.connection_no)
            {
                wk.clear();
            }
            return Ok(format_simple_string(String::from("OK")));
        }
        Ok(format_error_string("DISCARD without MULTI"))
    }
}
