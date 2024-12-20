use crate::frame::RequestFrame;
use atoi::atoi;
use bytes::Bytes;
use std::io::Cursor;
use thiserror::Error;

/// Utility for parsing a command
///
/// Commands are represented as a space delimited line. Each entry in the frame is a
/// "token". A `Parse` is initialized with a `RequestFrame` and provides a
/// cursor-like API. Each command struct includes a `parse_frame` method that
/// uses a `Parse` to extract its fields.
#[derive(Debug)]
pub(crate) struct Parse(Cursor<Bytes>);

/// Error encountered while parsing a `RequestFrame`.
#[derive(Error, Debug, PartialEq)]
pub(crate) enum ParseError {
    /// Attempting to extract a value failed due to the frame being fully
    /// consumed.
    #[error("protocol error; unexpected end of line")]
    EndOfLine,
    /// Line should have been consumed but there was data remaining
    #[error("protocol error; expected end of line, but there was more")]
    LineToLong,
    #[error("protocol error; invalid string")]
    String,
    #[error("protocol error; invalid u32")]
    U32,
    #[error("protocol error; invalid u64")]
    U64,
}

impl Parse {
    /// Create a new `Parse` to parse the command line.
    pub(crate) fn new(command_line: Bytes) -> Parse {
        Parse(Cursor::new(command_line))
    }

    /// Return the next entry by spilting on SPACE
    fn next(&mut self) -> Result<&[u8], ParseError> {
        let current_position = self.0.position() as usize;

        // Skips the first byte which should never be a SPACE
        let start = self.0.position() as usize + 1;
        // Scan to the second to last byte
        let end = self.0.get_ref().len() - 1;

        for i in start..end {
            if self.0.get_ref()[i] == b' ' {
                // Moves the position to after the SPACE
                self.0.set_position(i as u64 + 1);
                return Ok(&self.0.get_ref()[current_position..i]);
            }
        }
        // Gets data from last SPACE to the end of line
        if current_position < self.0.get_ref().len() {
            return Ok(&self.0.get_ref()[current_position..self.0.get_ref().len()]);
        }

        Err(ParseError::EndOfLine)
    }

    /// Return the next entry as a string.
    ///
    /// If the next entry cannot be represented as a String, then an error is returned.
    pub(crate) fn next_string(&mut self) -> Result<String, ParseError> {
        // Try str when this is working
        match String::from_utf8(self.next()?.to_vec()) {
            Ok(s) => Ok(s),
            Err(_) => Err(ParseError::String),
        }
    }

    /// Return the next entry as raw bytes.
    ///
    /// If the next entry cannot be represented as raw bytes, an error is
    /// returned.
    pub(crate) fn next_bytes(&mut self) -> Result<Bytes, ParseError> {
        Ok(Bytes::copy_from_slice(self.next()?))
    }

    /// Return the next entry as an u32.
    ///
    /// If the next entry cannot be represented as u32, then an error is returned.
    pub(crate) fn next_u32(&mut self) -> Result<u32, ParseError> {
        atoi::<u32>(self.next()?).ok_or_else(|| ParseError::U32)
    }

    /// Return the next entry as an u64.
    ///
    /// If the next entry cannot be represented as u64, then an error is returned.
    pub(crate) fn next_u64(&mut self) -> Result<u64, ParseError> {
        atoi::<u64>(self.next()?).ok_or_else(|| ParseError::U64)
    }

    /// Checks if there is more in the line
    pub(crate) fn complete(&mut self) -> bool {
        // use cusor is_empty when added
        // try self.0.has_remaining()
        self.0.position() as usize > self.0.get_ref().len()
    }

    /// Ensure there is no more data in the line
    pub(crate) fn finish(&mut self) -> Result<(), ParseError> {
        // use cusor is_empty when added
        // try self.0.has_remaining()
        if self.0.position() as usize > self.0.get_ref().len() {
            Ok(())
        } else {
            Err(ParseError::LineToLong)
        }
    }
}