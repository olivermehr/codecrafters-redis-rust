use super::{CommandArgs, RespError, RespServer};
use crate::{format_array, format_bulk_string, format_null_array, get_bulk_string_arg};
use std::time::Duration;
use tokio::sync::oneshot;

impl RespServer {
    pub(crate) async fn blpop(&mut self, mut v: CommandArgs) -> Result<String, RespError> {
        let list_name = get_bulk_string_arg(v.next())?;
        let timeout = get_bulk_string_arg(v.next())?.parse::<f32>().unwrap() * 1000.0;

        let front_pop = self.blpop_front(&list_name);
        match front_pop {
            Some(el) => Ok(format_array(&[
                format_bulk_string(list_name),
                format_bulk_string(el.into()),
            ])),
            None => {
                let (tx, rx) = oneshot::channel();
                self.storage
                    .lock()
                    .unwrap()
                    .get_mut(&list_name)
                    .unwrap()
                    .queue
                    .push_back(tx);
                if timeout == 0.0 {
                    let value = rx.await.unwrap();
                    Ok(format_array(&[
                        format_bulk_string(list_name),
                        format_bulk_string(value.into()),
                    ]))
                } else if timeout > 0.0 {
                    tokio::select! {
                        _ = tokio::time::sleep(Duration::from_millis(timeout as u64)) => {

                    Ok(format_null_array())


                        }
                         msg = rx => {
                            let value = msg.unwrap();
                            Ok(format_array(&[
                        format_bulk_string(list_name),
                        format_bulk_string(value.into()),
                    ]))


                        }
                    }
                } else {
                    panic!("negative timeout")
                }
            }
        }
    }
}
