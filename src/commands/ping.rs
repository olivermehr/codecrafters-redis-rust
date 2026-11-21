use crate::CommandResponse;
use super::{RespError, RespServer};
use crate::format_simple_string;

impl RespServer {
    pub(crate) async fn ping(&mut self) -> Result<CommandResponse, RespError> {
        Ok(CommandResponse::Single(format_simple_string(String::from("PONG"))))
    }
}
