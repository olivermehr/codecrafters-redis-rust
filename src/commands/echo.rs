use super::{RespError, RespServer};
use crate::CommandResponse;
use crate::commands::CommandFnArgs;
use crate::{format_bulk_string, get_bulk_string_arg};

impl RespServer {
    pub(crate) async fn echo(
        &mut self,
        CommandFnArgs {
            mut args,
            cmd_string: _,
        }: CommandFnArgs,
    ) -> Result<CommandResponse, RespError> {
        let value = get_bulk_string_arg(args.next())?;
        Ok(CommandResponse::Single(format_bulk_string(value)))
    }
}
