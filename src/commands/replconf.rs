use crate::{CommandResponse, ServerRole, commands::CommandFnArgs, format_bulk_string};
use std::collections::HashSet;

use crate::{
    RespServer, Storage, StorageType, format_array, format_error_string, format_integer_string,
    format_simple_string, get_bulk_string_arg, resp::error::RespError,
};

impl RespServer {
    pub async fn replconf(
        &mut self,
        CommandFnArgs {
            mut args,
            cmd_string,
        }: CommandFnArgs,
    ) -> Result<CommandResponse, RespError> {
        let arg = get_bulk_string_arg(args.next())?;
        if arg.eq_ignore_ascii_case("getack")
            && let ServerRole::Slave {
                master_host,
                master_port,
                offset,
                state,
            } = &self.role
        {
            let response = Ok(CommandResponse::ReplicaResponse(Box::new(
                CommandResponse::Single(format_array(&[
                    format_bulk_string(String::from("REPLCONF")),
                    format_bulk_string(String::from("ACK")),
                    format_bulk_string(
                        offset
                            .fetch_add(cmd_string.len(), std::sync::atomic::Ordering::Relaxed)
                            .to_string(),
                    ),
                ])),
            )));

            return response;
        } else if arg.eq_ignore_ascii_case("ack")
            && let ServerRole::Master {
                replid,
                master_offset,
                connected_slaves,
                replicas,
                ack_notifier,
            } = &self.role
        {
            let offset: usize = get_bulk_string_arg(args.next())?.parse().unwrap();
            println!(
                "received offset from conn index {} with offset {}",
                self.connection_no, offset
            );
            replicas
                .lock()
                .unwrap()
                .get_mut(&self.connection_no)
                .unwrap()
                .last_offset
                .swap(offset, std::sync::atomic::Ordering::Relaxed);
            ack_notifier.notify_waiters();
        }
        Ok(CommandResponse::Single(format_simple_string(String::from(
            "OK",
        ))))
    }
}
