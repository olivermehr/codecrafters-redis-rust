use crate::CommandResponse;
use crate::{
    RespServer, Storage, StorageType, format_array, format_error_string, format_integer_string,
    format_simple_string, get_bulk_string_arg, resp::error::RespError,
};

impl RespServer {
    pub async fn unwatch(&mut self) -> Result<CommandResponse, RespError> {
        if let Some(watched_keys) = self
            .watched_keys
            .lock()
            .unwrap()
            .get_mut(&self.connection_no)
        {
            watched_keys.clear();
        }
        Ok(CommandResponse::Single(format_simple_string(String::from(
            "OK",
        ))))
    }
}
