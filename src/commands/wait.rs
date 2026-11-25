use crate::CommandResponse;
use crate::{
    RespServer, Storage, StorageType, commands::CommandArgs, format_array, format_error_string,
    format_integer_string, format_simple_string, get_bulk_string_arg, resp::error::RespError,
};

impl RespServer {
    pub async fn wait(&mut self, mut v: CommandArgs) -> Result<CommandResponse, RespError> {
        let num_replicas = get_bulk_string_arg(v.next())?;
        let timeout = get_bulk_string_arg(v.next())?;
        Ok(CommandResponse::Single(format_integer_string(0)))
    }
}
