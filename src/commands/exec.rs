use crate::{
    RespServer, Storage, StorageType, commands::CommandArgs, format_array, format_error_string,
    format_integer_string, format_null_array, format_simple_string, get_bulk_string_arg,
    resp::error::RespError,
};

impl RespServer {
    pub async fn exec(&mut self) -> Result<String, RespError> {
        let commands;
        {
            // scope used to drop lock to not hold the mutable ref so we can call visit()
            let mut lock = self.command_queue.lock().unwrap();
            commands = match lock.get_mut(&self.connection_no) {
                Some(c) => c.take(),
                None => return Ok(format_error_string("EXEC without MULTI")),
            };
        }

        if let Some(commands) = commands {
            if let Some(trigger) = self
                .watched_keys
                .lock()
                .unwrap()
                .get_mut(&self.connection_no)
            {
                for (_, changed) in trigger.drain(..) {
                    if changed {
                        return Ok(format_null_array());
                    }
                }
            }
            if commands.is_empty() {
                return Ok(format_array(&[]));
            }
            let mut response_vec = Vec::new();
            for node in commands {
                response_vec.push(self.visit(node).await?)
            }
            return Ok(format_array(&response_vec));
        }
        Ok(format_error_string("EXEC without MULTI"))
    }
}
