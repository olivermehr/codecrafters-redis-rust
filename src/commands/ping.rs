use super::{RespError, RespServer};
use crate::format_simple_string;

impl RespServer {
    pub(crate) async fn ping(&mut self) -> Result<String, RespError> {
        Ok(format_simple_string(String::from("PONG")))
    }
}
