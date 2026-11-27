use super::{RespError, RespServer};
use crate::CommandResponse;
use crate::commands::CommandFnArgs;
use crate::{format_array, format_bulk_string, format_null_array, get_bulk_string_arg};
use std::time::Duration;
use tokio::sync::oneshot;

impl RespServer {
    pub(crate) async fn blpop(
        &mut self,
        CommandFnArgs {
            mut args,
            cmd_string,
        }: CommandFnArgs,
    ) -> Result<CommandResponse, RespError> {
        let list_name = get_bulk_string_arg(args.next())?;
        let timeout = get_bulk_string_arg(args.next())?.parse::<f32>().unwrap() * 1000.0;
        let adjusted_cmd_str = format_array(&[
            format_bulk_string(String::from("lpop")),
            format_bulk_string(list_name.clone()),
        ]);

        let front_pop = self.blpop_front(&list_name);
        match front_pop {
            Some(el) => {
                // replicate lpop
                self.replicate_command(adjusted_cmd_str).await;
                Ok(CommandResponse::Single(format_array(&[
                    format_bulk_string(list_name),
                    format_bulk_string(el.into()),
                ])))
            }
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
                    self.replicate_command(adjusted_cmd_str).await;
                    Ok(CommandResponse::Single(format_array(&[
                        format_bulk_string(list_name),
                        format_bulk_string(value.into()),
                    ])))
                } else if timeout > 0.0 {
                    tokio::select! {
                        _ = tokio::time::sleep(Duration::from_millis(timeout as u64)) => {

                    Ok(CommandResponse::Single(format_null_array()))


                        }
                         msg = rx => {
                            let value = msg.unwrap();
                            self.replicate_command(adjusted_cmd_str).await;
                            Ok(CommandResponse::Single(format_array(&[
                        format_bulk_string(list_name),
                        format_bulk_string(value.into()),
                    ])))


                        }
                    }
                } else {
                    panic!("negative timeout")
                }
            }
        }
    }
}
