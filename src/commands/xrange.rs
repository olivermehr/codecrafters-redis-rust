use super::{RespError, RespServer};
use crate::CommandResponse;
use crate::commands::CommandFnArgs;
use crate::{EMPTY_ARRAY, StorageType, format_array, format_stream_entries, get_bulk_string_arg};

impl RespServer {
    pub(crate) async fn xrange(
        &mut self,
        CommandFnArgs {
            mut args,
            cmd_string,
        }: CommandFnArgs,
    ) -> Result<CommandResponse, RespError> {
        let stream_key = get_bulk_string_arg(args.next())?;
        let mut start_index = get_bulk_string_arg(args.next())?;
        let mut stop_index = get_bulk_string_arg(args.next())?;
        let lock = self.storage.lock().unwrap();
        let storage = lock.get(&stream_key);
        if let Some(storage) = storage
            && let StorageType::Stream(v) = &storage.content
            && !v.is_empty()
        {
            let start_index = if start_index == "-" {
                v.first().unwrap().id.clone()
            } else if start_index.contains('-') {
                start_index
            } else {
                start_index.push_str("-0");
                start_index
            };
            let stop_index = if stop_index == "+" {
                v.last().unwrap().id.clone()
            } else if stop_index.contains('-') {
                stop_index
            } else {
                let milli_index = v
                    .binary_search_by_key(&stop_index.as_str(), |entry| {
                        entry.id.split_once('-').unwrap().0
                    })
                    .unwrap();
                let mut max_seq = 0;
                for i in &v[milli_index..] {
                    let (curr_milli, seq) = i.id.split_once('-').unwrap();
                    let seq = seq.parse().unwrap();
                    if curr_milli == stop_index && seq >= max_seq {
                        max_seq = seq;
                    } else {
                        break;
                    }
                }
                stop_index.push_str(&format!("-{}", max_seq));
                stop_index
            };
            let start_index = v
                .binary_search_by_key(&start_index.as_str(), |entry| entry.id.as_str())
                .unwrap();
            let stop_index = v
                .binary_search_by_key(&stop_index.as_str(), |entry| entry.id.as_str())
                .unwrap();
            return Ok(CommandResponse::Single(format_stream_entries(
                &v[start_index..=stop_index],
            )));
        }
        Ok(CommandResponse::Single(format_array(EMPTY_ARRAY)))
    }
}
