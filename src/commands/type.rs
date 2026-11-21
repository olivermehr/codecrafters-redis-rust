use crate::CommandResponse;
use super::{CommandArgs, RespError, RespServer};
use crate::{format_simple_string, get_bulk_string_arg};

impl RespServer {
    pub(crate) async fn r#type(&mut self, mut v: CommandArgs) -> Result<CommandResponse, RespError> {
        let key = get_bulk_string_arg(v.next())?;
        match self.storage.lock().unwrap().get(&key) {
            Some(v) => Ok(CommandResponse::Single(format_simple_string(v.content.to_string()))),
            None => Ok(CommandResponse::Single(format_simple_string(String::from("none")))),
        }
    }
}
