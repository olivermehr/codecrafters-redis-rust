use crate::CommandResponse;
use std::collections::{HashMap, HashSet};

use crate::{
    RespServer, Storage, StorageType, commands::CommandArgs, format_array, format_bulk_string,
    format_error_string, format_integer_string, format_simple_string, get_bulk_string_arg,
    get_bulk_string_arg_optional, resp::error::RespError,
};

static INFO_SECTIONS: &[&str] = &["server", "clients", "memory", "replication"];

impl RespServer {
    pub async fn info(&mut self, mut v: CommandArgs) -> Result<CommandResponse, RespError> {
        let optional_param = get_bulk_string_arg_optional(v.next());
        let mut info_map = HashMap::new();
        for section in INFO_SECTIONS {
            let mut section_info = format!("#{section}\n");
            if *section == "replication" {
                section_info.push_str(&self.role.to_string());
                info_map.insert(section, section_info);
            }
        }
        if let Some(optional_section) = optional_param {
            return Ok(CommandResponse::Single(format_bulk_string(
                info_map.remove(&optional_section.as_str()).unwrap(),
            )));
        }
        Ok(CommandResponse::Single(info_map.into_values().collect()))
    }
}
