use crate::{cache::Cache, frame::ResponseFrame, parse::Parse, Connection};
use anyhow::Result;
use log::debug;

/// Get the value of key.
///
/// If the key does not exist the special value nil is returned. An error is
/// returned if the value stored at key is not a string, because GET only
/// handles string values.
#[derive(Debug)]
pub struct Get {
    keys: Vec<String>,
}

impl Get {
    /// Create a new `Get` command which fetches `key`.
    pub fn new(keys: Vec<String>) -> Get {
        Get { keys }
    }

    // /// Get the key
    // pub fn key(&self) -> &str {
    //     &self.key
    // }

    /// Parse a `Get` instance from a received frame.
    ///
    /// The `Parse` argument provides a cursor-like API to read fields from the
    /// `Frame`. At this point, the entire frame has already been received from
    /// the socket.
    ///
    /// The `GET` string has already been consumed.
    ///
    /// # Returns
    ///
    /// Returns the `Get` value on success. If the frame is malformed, `Err` is
    /// returned.
    ///
    /// # Format
    ///
    /// Expects an array frame containing two entries.
    ///
    /// ```text
    /// GET key
    /// ```
    pub(crate) fn parse_frame(parse: &mut Parse) -> Result<Get> {
        let mut keys = vec![parse.next_string()?];

        while !parse.complete() {
            keys.push(parse.next_string()?)
        }

        Ok(Get { keys })
    }

    /// Apply the `Get` command to the specified `Cache` instance.
    ///
    /// The response is written to `dst`. This is called by the server in order
    /// to execute a received command.
    pub(crate) async fn apply(self, cache: Cache, dst: &mut Connection) -> Result<()> {
        // If there is only one key skip loop
        if self.keys.len() == 1 {
            let key = &self.keys[0];
            
            if let Some(item) = cache.get(&key).await {
                let frame = ResponseFrame::Value {
                    key: key.clone(),
                    flags: item.flags,
                    data_length: item.data.len(),
                    cas: None,
                    data: item.data,
                };
                debug!("{:?}", frame);
                dst.write_and_end(frame).await?;
            }
            return Ok(());
        }

        for key in self.keys {
            if let Some(item) = cache.get(&key).await {
                let frame = ResponseFrame::Value {
                    key,
                    flags: item.flags,
                    data_length: item.data.len(),
                    cas: None,
                    data: item.data,
                };
                debug!("{:?}", frame);
                dst.write(frame);
            }
        }

        dst.end_and_flush().await?;
        Ok(())
    }
}
