use anyhow::Error;
use bytes::{Buf, Bytes};
use std::io::Cursor;

fn get_line<'a>(src: &mut Cursor<&'a [u8]>) -> Result<&'a [u8], Error> {
    // Maybe skip 3 or 4 bytes
    // Scan the bytes directly
    let start = src.position() as usize;
    // Scan to the second to last byte
    let end = src.get_ref().len() - 1;

    for i in start..end {
        if src.get_ref()[i] == b'\r' && src.get_ref()[i + 1] == b'\n' {
            // We found a line, update the position to be *after* the "\n"
            src.set_position((i + 2) as u64);

            // Return the line without "\r\n"
            return Ok(&src.get_ref()[start..i]);
        }
    }
    // Err(Error::Incomplete)
    Err(Error::msg("Incomplete"))
}

/// Storage commands use two lines. The first is the command and the second is data.
/// These commands are "set", "add", "replace", "append", "prepend", or "cas"
#[derive(Clone, Debug)]
pub struct StorageFrame {
    pub command_line: Bytes,
    pub data: Bytes,
}

#[derive(Clone, Debug)]
pub enum RequestFrame {
    Storage(StorageFrame),
    Other(Bytes),
}

// #[derive(Debug)]
// pub enum Error {
//     /// Not enough data is available to parse a message
//     Incomplete,

// }

impl RequestFrame {
    /// Checks if an entire message can be decoded from `src`
    pub fn check(src: &mut Cursor<&[u8]>) -> Result<(), Error> {
        match get_first_byte(src)? {
            b's' | b'a' | b'r' | b'p' | b'c' => {
                get_line(src)?;
                get_line(src)?;
            }
            _ => {
                get_line(src)?;
            }
        }
        Ok(())
    }

    /// The message has already been validated with `check`.
    pub fn parse(src: &mut Cursor<&[u8]>) -> Result<RequestFrame, Error> {
        match get_first_byte(src)? {
            b's' | b'a' | b'r' | b'p' | b'c' => {
                let command_line = Bytes::copy_from_slice(get_line(src)?);
                let data = Bytes::copy_from_slice(get_line(src)?);

                Ok(RequestFrame::Storage(StorageFrame { command_line, data }))
            }
            _ => Ok(RequestFrame::Other(Bytes::copy_from_slice(get_line(src)?))),
        }
    }

    // Converts the frame to an "unexpected frame" error
    // pub(crate) fn to_error(&self) -> Error {
    //     Error::msg(format!("unexpected frame: {}", self))
    // }
}

fn get_first_byte(src: &mut Cursor<&[u8]>) -> Result<u8, Error> {
    if !src.has_remaining() {
        return Err(Error::msg("Incomplete"));
    }

    Ok(src.get_u8())
}

#[derive(Clone, Debug)]
pub enum ResponseFrame {
    Value {
        key: String,
        flags: u32,
        data_length: usize,
        cas: Option<u64>,
        data: Bytes
    },
    Crement(usize), // Result of increment or decrement
    Deleted,
    Stored,
    Touched,
    NotFound,
    NotStored,
    Exists,
    ClientError(String),
    ServerError(String),
    Error,
}