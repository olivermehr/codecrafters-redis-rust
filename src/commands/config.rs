use crate::{CommandResponse, commands::CommandFnArgs, format_bulk_string};
use std::collections::HashSet;

use crate::{
    RespServer, Storage, StorageType, format_array, format_error_string, format_integer_string,
    format_simple_string, get_bulk_string_arg, resp::error::RespError,
};

impl RespServer {
    pub async fn config(
        &mut self,
        CommandFnArgs {
            mut args,
            cmd_string,
        }: CommandFnArgs,
    ) -> Result<CommandResponse, RespError> {
        let req_type = get_bulk_string_arg(args.next())?;
        if req_type.eq_ignore_ascii_case("get") {
            let mut results = Vec::new();
            let mut params: Vec<String> = args
                .map(|a| get_bulk_string_arg(Some(a)))
                .collect::<Result<Vec<String>, RespError>>()?;
            let config = self.config.lock().unwrap();
            for p in params {
                if let Some(c) = config.get(&p) {
                    results.push(format_bulk_string(p));
                    results.push(format_bulk_string(c.clone()));
                }
            }
            return Ok(CommandResponse::Single(format_array(&results)));
        }
        panic!("unsupported config req type")
    }
}
