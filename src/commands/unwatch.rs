use crate::{
    RespServer, Storage, StorageType, commands::CommandArgs, format_array, format_error_string,
    format_integer_string, format_simple_string, get_bulk_string_arg, resp::error::RespError,
};

impl RespServer {
    pub async fn unwatch(&mut self) -> Result<String, RespError> {
        if let Some(watched_keys) = self
            .watched_keys
            .lock()
            .unwrap()
            .get_mut(&self.connection_no)
        {
            watched_keys.clear();
        }
        Ok(format_simple_string(String::from("OK")))
    }
}
