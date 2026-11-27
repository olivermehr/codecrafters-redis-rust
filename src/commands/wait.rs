use std::collections::HashMap;
use std::num;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::mpsc::Sender;

use crate::commands::CommandFnArgs;
use crate::{CommandResponse, Replica, ServerRole, format_bulk_string};
use crate::{
    RespServer, Storage, StorageType, format_array, format_error_string, format_integer_string,
    format_simple_string, get_bulk_string_arg, resp::error::RespError,
};

impl RespServer {
    pub async fn wait(
        &mut self,
        CommandFnArgs {
            mut args,
            cmd_string,
        }: CommandFnArgs,
    ) -> Result<CommandResponse, RespError> {
        let num_replicas: usize = get_bulk_string_arg(args.next())?.parse().unwrap();
        let timeout: u64 = get_bulk_string_arg(args.next())?.parse().unwrap();
        if let ServerRole::Master {
            replid,
            master_offset,
            connected_slaves,
            replicas,
            ack_notifier,
        } = &self.role
        {
            if connected_slaves.load(std::sync::atomic::Ordering::Relaxed) == 0 || num_replicas == 0
            {
                return Ok(CommandResponse::Single(format_integer_string(0)));
            }
            {
                let response_string = format_array(&[
                    format_bulk_string(String::from("REPLCONF")),
                    format_bulk_string(String::from("GETACK")),
                    format_bulk_string(String::from("*")),
                ]);
                // get current offset and increment by the repl conf
                let current_offset = master_offset
                    .fetch_add(response_string.len(), std::sync::atomic::Ordering::Relaxed);
                println!("wait command current offset {}", current_offset);
                self.replicate_command(response_string).await;

                let mut replicas_acked = 0;

                let notify_loop = async {
                    loop {
                        ack_notifier.notified().await;
                        replicas_acked = check_replica_offset(replicas, current_offset);

                        if replicas_acked >= num_replicas {
                            break;
                        }
                    }
                };

                tokio::select! {
                    _ = notify_loop => {},
                    _ = tokio::time::sleep(Duration::from_millis(timeout)) => {}

                };
                return Ok(CommandResponse::Single(format_integer_string(
                    replicas_acked as isize,
                )));
            }
        }
        Ok(CommandResponse::Single(format_error_string(
            "WAIT command must be sent to master",
        )))
    }
}
fn check_replica_offset(
    replicas: &Arc<Mutex<HashMap<usize, Replica>>>,
    target_offset: usize,
) -> usize {
    let mut count = 0;
    replicas.lock().unwrap().values().for_each(|r| {
        println!(
            "offset of replica {}",
            r.last_offset.load(std::sync::atomic::Ordering::Relaxed)
        );
        if r.last_offset.load(std::sync::atomic::Ordering::Relaxed) >= target_offset {
            count += 1;
        }
    });
    count
}
