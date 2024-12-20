use crate::{
    cache::{Cache, Item},
    frame::ResponseFrame,
    parse::Parse,
    Connection,
};
use anyhow::Result;
use bytes::Bytes;
use log::debug;
use std::time::Duration;

/// Set `key` to hold the string `value`.
///
/// If `key` already holds a value, it is overwritten, regardless of its type.
/// Any previous time to live associated with the key is discarded on successful
/// SET operation.
///
/// # Options
///
/// Currently, the following options are supported:
///
/// * EX `seconds` -- Set the specified expire time, in seconds.
/// * PX `milliseconds` -- Set the specified expire time, in milliseconds.
#[derive(Debug)]
pub struct Set {
    pub key: String,
    pub flags: u32,
    pub cas: u64,
    pub expiration: Option<u32>,
    pub data: Bytes,
}

impl Set {
    /// Create a new `Set` command which sets `key` to `value`.
    ///
    /// If `expire` is `Some`, the value should expire after the specified
    /// duration.
    pub fn new(key: String, flags: u32, expiration: Option<u32>, data: Bytes) -> Set {
        Set {
            key,
            flags,
            expiration,
            cas: 0,
            data,
        }
    }

    pub(crate) fn parse_frame(parse: &mut Parse, data: Bytes) -> Result<Set> {
        // Read the key to set. This is a required field
        let key = parse.next_string()?;

        // Read the value to set. This is a required field.
        let flags = parse.next_u32()?;

        // ToDo: convert expiration
        let expiration = parse.next_u32()?;

        let _ = parse.next_u32()?; // data_length

        Ok(Set { key, flags, cas: 0, expiration: Some(expiration), data })
    }

    /// Apply the `Set` command to the specified `Db` instance.
    ///
    /// The response is written to `dst`. This is called by the server in order
    /// to execute a received command.
    pub(crate) async fn apply(self, cache: Cache, dst: &mut Connection) -> Result<()> {
        // Set the value in the shared database state.
        cache.set(self.key, self.flags, self.expiration, self.data);

        // Create a success response and write it to `dst`.
        let response = ResponseFrame::Stored;
        debug!("{:?}", response);
        dst.write_and_flush(response).await?;

        Ok(())
    }
}
