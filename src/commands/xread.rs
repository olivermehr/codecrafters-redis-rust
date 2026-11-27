use super::{RespError, RespServer};
use crate::CommandResponse;
use crate::commands::CommandFnArgs;
use crate::{
    StorageType, format_array, format_bulk_string, format_null_array, format_stream_entries,
    get_bulk_string_arg,
};
use futures::stream::{FuturesUnordered, StreamExt};
use std::time::Duration;
use tokio::sync::oneshot;

impl RespServer {
    pub(crate) async fn xread(
        &mut self,
        CommandFnArgs {
            mut args,
            cmd_string,
        }: CommandFnArgs,
    ) -> Result<CommandResponse, RespError> {
        let mut param_vec = Vec::new();
        while let Ok(p) = get_bulk_string_arg(args.next()) {
            param_vec.push(p)
        }
        let mut block: Option<usize> = None;

        if let Some(pos) = param_vec.iter().position(|p| p.as_str() == "streams") {
            param_vec.remove(pos);
        }
        if let Some(pos) = param_vec.iter().position(|p| p.as_str() == "block") {
            param_vec.remove(pos);
            block.replace(param_vec.remove(pos).parse().unwrap());
        }

        assert!(param_vec.len() % 2 == 0);
        let param_mid = param_vec.len() / 2;
        let mut block_vec = Vec::new();
        let mut stream_keys_vec = Vec::new();
        let mut stream_vec = Vec::new();
        // adding this scope to show the compiler that the mutex guard is dropped
        {
            let mut lock = self.storage.lock().unwrap();

            for i in 0..param_mid {
                let stream_key = &param_vec[i];
                let storage = lock.get_mut(stream_key);
                if let Some(storage) = storage
                    && let StorageType::Stream(v) = &storage.content
                    && !v.is_empty()
                {
                    let start_id: &String = &param_vec[i + param_mid];
                    let index = if start_id == "$" && block.is_some() {
                        v.len()
                    } else {
                        match v.binary_search_by_key(&start_id.as_str(), |e| e.id.as_str()) {
                            Ok(i) => i + 1,
                            Err(i) => i,
                        }
                    };

                    if index == v.len() && block.is_some() {
                        let (tx, rx) = oneshot::channel();
                        storage.queue.push_back(tx);
                        block_vec.push(rx);
                        stream_keys_vec.push(stream_key.clone());
                    } else if index < v.len() && block.is_some() {
                        return Ok(CommandResponse::Single(format_array(&[
                            format_bulk_string(stream_key.clone()),
                            format_stream_entries(&v[index..]),
                        ])));
                    } else {
                        stream_vec.push(format_array(&[
                            format_bulk_string(stream_key.clone()),
                            format_stream_entries(&v[index..]),
                        ]));
                    }
                }
            }
        }
        if let Some(millis) = block {
            let mut futures = FuturesUnordered::new();
            for i in block_vec {
                futures.push(i)
            }
            if millis > 0 {
                tokio::select! {
                se = futures.next() => {
                    if let Some(Ok(StorageType::Stream(mut v))) = se {
                        let entry = v.remove(0);
                        return Ok(CommandResponse::Single(format_array(&[format_array(&[
                        format_bulk_string(entry.stream_key.clone()),
                        format_stream_entries(&[entry]),
                    ])])));

                    }

                    },
                _ = tokio::time::sleep(Duration::from_millis(millis as u64)) => {

                    return Ok(CommandResponse::Single(format_null_array()))

                }

                }
            } else {
                let se = futures.next().await;
                if let Some(Ok(StorageType::Stream(mut v))) = se {
                    let entry = v.remove(0);
                    return Ok(CommandResponse::Single(format_array(&[format_array(&[
                        format_bulk_string(entry.stream_key.clone()),
                        format_stream_entries(&[entry]),
                    ])])));
                }
            }
        }
        Ok(CommandResponse::Single(format_array(&stream_vec)))
    }
}
