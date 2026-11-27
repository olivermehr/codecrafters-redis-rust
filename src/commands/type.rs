use super::{RespError, RespServer};
use crate::CommandResponse;
use crate::commands::CommandFnArgs;
use crate::{format_simple_string, get_bulk_string_arg};

impl RespServer {
    pub(crate) async fn r#type(
        &mut self,
        CommandFnArgs {
            mut args,
            cmd_string,
        }: CommandFnArgs,
    ) -> Result<CommandResponse, RespError> {
        let key = get_bulk_string_arg(args.next())?;
        match self.storage.lock().unwrap().get(&key) {
            Some(v) => Ok(CommandResponse::Single(format_simple_string(
                v.content.to_string(),
            ))),
            None => Ok(CommandResponse::Single(format_simple_string(String::from(
                "none",
            )))),
        }
    }
}
