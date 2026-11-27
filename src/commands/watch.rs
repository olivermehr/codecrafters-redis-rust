use crate::{CommandResponse, commands::CommandFnArgs};
use std::collections::HashSet;

use crate::{
    RespServer, Storage, StorageType, format_array, format_error_string, format_integer_string,
    format_simple_string, get_bulk_string_arg, resp::error::RespError,
};

impl RespServer {
    pub async fn watch(
        &mut self,
        CommandFnArgs {
            mut args,
            cmd_string,
        }: CommandFnArgs,
    ) -> Result<CommandResponse, RespError> {
        let mut keys = Vec::new();
        while let Ok(k) = get_bulk_string_arg(args.next()) {
            keys.push(k);
        }

        match self.command_queue.lock().unwrap().get(&self.connection_no) {
            Some(Some(_)) => Ok(CommandResponse::Single(format_error_string(
                "WATCH inside MULTI is not allowed",
            ))),
            _ => {
                let mut watched_keys = self.watched_keys.lock().unwrap();
                let watched_keys = watched_keys.entry(self.connection_no).or_default();
                for key in keys {
                    watched_keys.push((key, false));
                }

                Ok(CommandResponse::Single(format_simple_string(String::from(
                    "OK",
                ))))
            }
        }
    }
}
