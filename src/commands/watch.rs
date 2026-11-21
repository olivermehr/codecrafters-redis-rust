use crate::{
    RespServer, Storage, StorageType, commands::CommandArgs, format_array, format_error_string,
    format_integer_string, format_simple_string, get_bulk_string_arg, resp::error::RespError,
};

impl RespServer {
    pub async fn watch(&mut self, mut v: CommandArgs) -> Result<String, RespError> {
        let key_name = get_bulk_string_arg(v.next())?;
        match self.command_queue.lock().unwrap().get(&self.connection_no) {
            Some(Some(_)) => Ok(format_error_string("WATCH inside MULTI is not allowed")),
            _ => {
                self.storage
                    .lock()
                    .unwrap()
                    .entry(key_name)
                    .or_insert(Storage::new(StorageType::String(String::new())))
                    .watched
                    .insert(self.connection_no);

                Ok(format_simple_string(String::from("OK")))
            }
        }
    }
}
