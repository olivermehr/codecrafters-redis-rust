use super::{CommandArgs, Expiry, RespError, RespServer};
use crate::{Storage, format_simple_string, get_bulk_string_arg, get_bulk_string_arg_optional};
use tokio::sync::mpsc::Sender;

impl RespServer {
    pub(crate) async fn set(&mut self, mut v: CommandArgs) -> Result<String, RespError> {
        let key = get_bulk_string_arg(v.next())?;
        let value = get_bulk_string_arg(v.next())?;
        while let Some(arg) = get_bulk_string_arg_optional(v.next()) {
            if arg == "ex" {
                let num = get_bulk_string_arg(v.next())?;
                self.expiry_tx
                    .send(Expiry {
                        key: key.clone(),
                        millis: num.parse::<u64>().map_err(|_| RespError::IncorrectParams)? * 1000,
                    })
                    .await
                    .unwrap();
            } else if arg == "px" {
                let num = get_bulk_string_arg(v.next())?;
                self.expiry_tx
                    .send(Expiry {
                        key: key.clone(),
                        millis: num.parse().map_err(|_| RespError::IncorrectParams)?,
                    })
                    .await
                    .unwrap();
            }
        }
        self.storage
            .lock()
            .unwrap()
            .insert(key, Storage::new(value.into()));
        Ok(format_simple_string(String::from("OK")))
    }
}
