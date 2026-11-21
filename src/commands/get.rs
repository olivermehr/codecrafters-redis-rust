use crate::CommandResponse;
use super::{CommandArgs, RespError, RespServer};
use crate::{format_bulk_string, format_null_string, get_bulk_string_arg};

impl RespServer {
    pub(crate) async fn get(&mut self, mut v: CommandArgs) -> Result<CommandResponse, RespError> {
        let key = get_bulk_string_arg(v.next())?;
        let value = self
            .storage
            .lock()
            .unwrap()
            .get(&key)
            .map(|storage| storage.content.clone());
        if let Some(v) = value {
            Ok(CommandResponse::Single(format_bulk_string(v.into())))
        } else {
            Ok(CommandResponse::Single(format_null_string()))
        }
    }
}
