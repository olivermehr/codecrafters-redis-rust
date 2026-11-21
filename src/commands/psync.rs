use crate::CommandResponse;
use std::collections::HashSet;

use crate::{
    RespServer, ServerRole, Storage, StorageType, commands::CommandArgs, format_array,
    format_error_string, format_integer_string, format_simple_string, get_bulk_string_arg,
    resp::error::RespError,
};

impl RespServer {
    pub async fn psync(&mut self, mut v: CommandArgs) -> Result<CommandResponse, RespError> {
        let slave_replicaton_id = get_bulk_string_arg(v.next())?;
        let slave_offset = get_bulk_string_arg(v.next())?;
        if let ServerRole::Master { replid, offset } = &self.role
            && slave_replicaton_id == "?"
            && slave_offset == "-1"
        {
            let rbd_file_hex = "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2";
            let rdb_file_bytes = rbd_file_hex.as_bytes().as_chunks::<2>();
            println!("{:?}", rbd_file_hex)
           
        }
        Err(RespError::InvalidSyntax)
    }
}
