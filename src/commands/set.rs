use super::{CommandArgs, Expiry, RespError, RespServer};
use crate::{
    Storage, StorageType, format_simple_string, get_bulk_string_arg, get_bulk_string_arg_optional,
};
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
        let mut lock = self.storage.lock().unwrap();
        match lock.get_mut(&key) {
            Some(val) => {
                val.content = StorageType::String(value);
            }
            None => {
                lock.insert(key.clone(), Storage::new(value.into()));
            }
        }
        if let Some(watched_keys) = self
            .watched_keys
            .lock()
            .unwrap()
            .get_mut(&self.connection_no)
        {
            for (wk, status) in watched_keys {
                if key == *wk {
                    *status = true
                }
            }
        }

        Ok(format_simple_string(String::from("OK")))
    }
}
