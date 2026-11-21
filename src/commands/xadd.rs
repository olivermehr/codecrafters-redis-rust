use crate::CommandResponse;
use super::{CommandArgs, RespError, RespServer};
use crate::{
    Storage, StorageType, StreamEntries, format_bulk_string, format_error_string,
    get_bulk_string_arg,
};

impl RespServer {
    pub(crate) async fn xadd(&mut self, mut v: CommandArgs) -> Result<CommandResponse, RespError> {
        let stream_key = get_bulk_string_arg(v.next())?;
        let mut id = get_bulk_string_arg(v.next())?;
        match self.validate_stream_id(&stream_key, id) {
            Ok(processed_id) => id = processed_id,
            Err(s) => return Ok(CommandResponse::Single(format_error_string(&s))),
        }
        let mut kp_vec = Vec::new();
        while let Ok(key) = get_bulk_string_arg(v.next()) {
            let value = get_bulk_string_arg(v.next())?;
            kp_vec.push((key, value));
        }
        let mut lock = self.storage.lock().unwrap();
        let stream = lock
            .entry(stream_key.clone())
            .or_insert(Storage::new(StorageType::Stream(Vec::new())));
        if let StorageType::Stream(v) = &mut stream.content {
            let se = StreamEntries {
                id: id.clone(),
                stream_key,
                data: kp_vec,
            };
            for sender in stream.queue.drain(..) {
                sender.send(StorageType::Stream(vec![se.clone()])).unwrap();
            }
            v.push(se);
        }
        Ok(CommandResponse::Single(format_bulk_string(id)))
    }
}
