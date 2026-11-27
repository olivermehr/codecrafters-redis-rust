use crate::CommandResponse;
use crate::commands::CommandFnArgs;
use crate::{
    RespServer, Storage, StorageType, format_error_string, format_integer_string,
    get_bulk_string_arg, resp::error::RespError,
};

impl RespServer {
    pub async fn incr(
        &mut self,
        CommandFnArgs {
            mut args,
            cmd_string,
        }: CommandFnArgs,
    ) -> Result<CommandResponse, RespError> {
        let key_name = get_bulk_string_arg(args.next())?;
        self.replicate_command(cmd_string).await;
        let mut lock = self.storage.lock().unwrap();
        match lock.get_mut(&key_name) {
            Some(val) => {
                if let StorageType::String(s) = &mut val.content {
                    let num = s.parse::<usize>();
                    match num {
                        Ok(mut n) => {
                            n += 1;
                            *s = n.to_string();
                            return Ok(CommandResponse::Single(format_integer_string(n as isize)));
                        }
                        Err(e) => {
                            return Ok(CommandResponse::Single(format_error_string(
                                "value is not an integer or out of range",
                            )));
                        }
                    }
                }
                panic!("unsupported storage type")
            }
            None => {
                lock.insert(key_name, Storage::new(StorageType::String(1.to_string())));
                Ok(CommandResponse::Single(format_integer_string(1)))
            }
        }
    }
}
