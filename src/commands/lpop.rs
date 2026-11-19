use super::{CommandArgs, RespError, RespServer};
use crate::{
    StorageType, format_bulk_string, format_null_string, get_bulk_string_arg, get_bulk_string_arg_optional, format_array,
};

impl RespServer {
    pub(crate) async fn lpop(&mut self, mut v: CommandArgs) -> Result<String, RespError> {
        let list_name = get_bulk_string_arg(v.next())?;
        let pop_num: usize = get_bulk_string_arg_optional(v.next())
            .map(|p| p.parse().unwrap())
            .unwrap_or(1);
        let mut map = self.storage.lock().unwrap();
        let storage = map.get_mut(&list_name);
        if let Some(storage) = storage
            && let StorageType::List(l) = &mut storage.content
        {
            let pop_num = if pop_num >= l.len() { l.len() } else { pop_num };
            let mut items = Vec::new();
            if pop_num == 0 {
                return Ok(format_null_string());
            }
            for _ in 0..pop_num {
                items.push(format_bulk_string(l.pop_front().unwrap().into()));
            }
            if pop_num == 1 {
                return Ok(items.into_iter().next().unwrap());
            }
            return Ok(format_array(&items));
        }
        Ok(format_null_string())
    }
}
