use super::{CommandArgs, RespError, RespServer};
use crate::{format_bulk_string, get_bulk_string_arg};

impl RespServer {
    pub(crate) async fn echo(&mut self, mut v: CommandArgs) -> Result<String, RespError> {
        let value = get_bulk_string_arg(v.next())?;
        Ok(format_bulk_string(value))
    }
}
