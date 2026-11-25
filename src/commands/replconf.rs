use crate::{CommandResponse, ServerRole, format_bulk_string};
use std::collections::HashSet;

use crate::{
    RespServer, Storage, StorageType, commands::CommandArgs, format_array, format_error_string,
    format_integer_string, format_simple_string, get_bulk_string_arg, resp::error::RespError,
};

impl RespServer {
    pub async fn replconf(&mut self, mut v: CommandArgs) -> Result<CommandResponse, RespError> {
        let arg = get_bulk_string_arg(v.next())?;
        if arg.eq_ignore_ascii_case("getack")
            && let ServerRole::Slave {
                master_host,
                master_port,
                offset,
            } = &self.role
        {
            return Ok(CommandResponse::ReplicaResponse(Box::new(
                CommandResponse::Single(format_array(&[
                    format_bulk_string(String::from("REPLCONF")),
                    format_bulk_string(String::from("ACK")),
                    format_bulk_string(
                        (offset.load(std::sync::atomic::Ordering::Relaxed)).to_string(),
                    ),
                ])),
            )));
        }
        Ok(CommandResponse::Single(format_simple_string(String::from(
            "OK",
        ))))
    }
}
