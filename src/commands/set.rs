use super::{Expiry, RespError, RespServer};
use crate::CommandResponse;
use crate::commands::CommandFnArgs;
use crate::{
    Storage, StorageType, format_simple_string, get_bulk_string_arg, get_bulk_string_arg_optional,
};
use tokio::sync::mpsc::Sender;

impl RespServer {
    pub(crate) async fn set(
        &mut self,
        CommandFnArgs {
            mut args,
            cmd_string,
        }: CommandFnArgs,
    ) -> Result<CommandResponse, RespError> {
        let key = get_bulk_string_arg(args.next())?;
        let value = get_bulk_string_arg(args.next())?;
        while let Some(arg) = get_bulk_string_arg_optional(args.next()) {
            if arg == "ex" {
                let num = get_bulk_string_arg(args.next())?;
                self.expiry_tx
                    .send(Expiry {
                        key: key.clone(),
                        millis: num.parse::<u64>().map_err(|_| RespError::IncorrectParams)? * 1000,
                    })
                    .await
                    .unwrap();
            } else if arg == "px" {
                let num = get_bulk_string_arg(args.next())?;
                self.expiry_tx
                    .send(Expiry {
                        key: key.clone(),
                        millis: num.parse().map_err(|_| RespError::IncorrectParams)?,
                    })
                    .await
                    .unwrap();
            }
        }

        self.replicate_command(cmd_string).await;
        let mut lock = self.storage.lock().unwrap();
        match lock.get_mut(&key) {
            Some(val) => {
                val.content = StorageType::String(value);
            }
            None => {
                lock.insert(key.clone(), Storage::new(value.into()));
            }
        }
        for all_watched_keys in self.watched_keys.lock().unwrap().values_mut() {
            for (wk, status) in all_watched_keys.iter_mut() {
                if *wk == key {
                    *status = true;
                }
            }
        }

        Ok(CommandResponse::Single(format_simple_string(String::from(
            "OK",
        ))))
    }
}
