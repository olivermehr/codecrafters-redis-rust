use crate::{
    RespServer, Storage, StorageType, commands::CommandArgs, format_array, format_error_string,
    format_integer_string, format_simple_string, get_bulk_string_arg, resp::error::RespError,
};

impl RespServer {
    pub async fn watch(&mut self, mut v: CommandArgs) -> Result<String, RespError> {
        let mut keys = Vec::new();
        while let Ok(k) = get_bulk_string_arg(v.next()) {
            keys.push(k);
        }

        match self.command_queue.lock().unwrap().get(&self.connection_no) {
            Some(Some(_)) => Ok(format_error_string("WATCH inside MULTI is not allowed")),
            _ => {
                let mut watched_keys = self.watched_keys.lock().unwrap();
                let watched_keys = watched_keys.entry(self.connection_no).or_insert(Vec::new());
                for key in keys {
                    watched_keys.push((key, false));
                }

                Ok(format_simple_string(String::from("OK")))
            }
        }
    }
}
