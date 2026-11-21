use std::collections::HashSet;

use crate::{
    RespServer, ServerRole, Storage, StorageType, commands::CommandArgs, format_array,
    format_error_string, format_integer_string, format_simple_string, get_bulk_string_arg,
    resp::error::RespError,
};

impl RespServer {
    pub async fn psync(&mut self, mut v: CommandArgs) -> Result<String, RespError> {
        let slave_replicaton_id = get_bulk_string_arg(v.next())?;
        let slave_offset = get_bulk_string_arg(v.next())?;
        if let ServerRole::Master { replid, offset } = &self.role
            && slave_replicaton_id == "?"
            && slave_offset == "-1"
        {
            return Ok(format_simple_string(format!("FULLRESYNC {} {}", replid, 0)));
        }
        Err(RespError::InvalidSyntax)
    }
}
