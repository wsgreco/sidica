use crate::frame::{RequestFrame, ResponseFrame};
use anyhow::{Error, Result};
use bytes::{Buf, BytesMut};
use std::io::Cursor;
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufWriter};
use tokio::net::TcpStream;

const READ_BUFFER_SIZE: usize = 4096;

//To read frames, the `Connection` uses an internal buffer, which is filled
/// up until there are enough bytes to create a full frame. Once this happens,
/// the `Connection` creates the frame and returns it to the caller.
///
/// When sending frames, the frame is first encoded into the write buffer.
/// The contents of the write buffer are then written to the socket.
#[derive(Debug)]
pub struct Connection {
    stream: BufWriter<TcpStream>,
    buffer: BytesMut,
}

impl Connection {
    pub fn new(socket: TcpStream) -> Connection {
        Connection {
            stream: BufWriter::new(socket),
            buffer: BytesMut::with_capacity(READ_BUFFER_SIZE),
        }
    }

    /// Read a single `Frame` value from the underlying stream.
    ///
    /// The function waits until it has retrieved enough data to parse a frame.
    /// Any data remaining in the read buffer after the frame has been parsed is
    /// kept there for the next call to `read_frame`.
    ///
    /// # Returns
    ///
    /// On success, the received frame is returned. If the `TcpStream`
    /// is closed in a way that doesn't break a frame in half, it returns
    /// `None`. Otherwise, an error is returned.
    pub async fn read_frame(&mut self) -> Result<Option<RequestFrame>> {
        loop {
            // Attempt to parse a frame from the buffered data. If enough data
            // has been buffered, the frame is returned.
            if let Some(frame) = self.parse_frame()? {
                return Ok(Some(frame));
            }

            // There is not enough buffered data to read a frame. Attempt to
            // read more data from the socket.
            //
            // On success, the number of bytes is returned. `0` indicates "end
            // of stream".
            let bytes_read = self.stream.read_buf(&mut self.buffer).await?;
            if bytes_read == 0 {
                // The remote closed the connection. For this to be a clean
                // shutdown, there should be no data in the read buffer. If
                // there is, this means that the peer closed the socket while
                // sending a frame.
                if self.buffer.is_empty() {
                    return Ok(None);
                } else {
                    return Err(Error::msg("connection reset by peer"));
                }
            }
        }
    }

    /// Tries to parse a frame from the buffer. If the buffer contains enough
    /// data, the frame is returned and the data removed from the buffer. If not
    /// enough data has been buffered yet, `Ok(None)` is returned. If the
    /// buffered data does not represent a valid frame, `Err` is returned.
    fn parse_frame(&mut self) -> Result<Option<RequestFrame>> {
        // use frame::Error::Incomplete;

        let mut buf = Cursor::new(&self.buffer[..]);

        // The first step is to check if enough data has been buffered to parse
        // a single frame. This step is usually much faster than doing a full
        // parse of the frame, and allows us to skip allocating data structures
        // to hold the frame data unless we know the full frame has been
        // received.
        match RequestFrame::check(&mut buf) {
            Ok(_) => {
                // The `check` function will have advanced the cursor until the
                // end of the frame. Since the cursor had position set to zero
                // before `Frame::check` was called, we obtain the length of the
                // frame by checking the cursor position.
                let len = buf.position() as usize;

                // Reset the position to zero before passing the cursor to
                // `Frame::parse`.
                buf.set_position(0);

                // Parse the frame from the buffer. This allocates the necessary
                // structures to represent the frame and returns the frame
                // value.
                //
                // If the encoded frame representation is invalid, an error is
                // returned. This should terminate the **current** connection
                // but should not impact any other connected client.
                let frame = RequestFrame::parse(&mut buf)?;

                // Discard the parsed data from the read buffer.
                //
                // When `advance` is called on the read buffer, all of the data
                // up to `len` is discarded. The details of how this works is
                // left to `BytesMut`. This is often done by moving an internal
                // cursor, but it may be done by reallocating and copying data.
                self.buffer.advance(len);

                Ok(Some(frame))
            }
            // There is not enough data present in the read buffer to parse a
            // single frame. We must wait for more data to be received from the
            // socket. Reading from the socket will be done in the statement
            // after this `match`.
            //
            // We do not want to return `Err` from here as this "error" is an
            // expected runtime condition.
            Err(Incomplete) => Ok(None),
            // An error was encountered while parsing the frame. The connection
            // is now in an invalid state. Returning `Err` from here will result
            // in the connection being closed.
            Err(e) => Err(e.into()),
        }
    }

    async fn write_value(&mut self, frame: ResponseFrame) -> Result<()> {
        use ResponseFrame::*;

        match frame {
            // Figure out better way to convert int to ascii
            Value {
                key,
                flags,
                data_length,
                cas,
                data,
            } => {
                self.stream.write_all(b"VALUE").await?;
                self.stream.write_all(key.as_bytes()).await?;
                self.stream.write_all(flags.to_string().as_bytes()).await?;
                self.stream
                    .write_all(data_length.to_string().as_bytes())
                    .await?;
                if let Some(cas) = cas {
                    self.stream.write_all(cas.to_string().as_bytes()).await?;
                }
                self.stream.write_all(b"\r\n").await?;
                self.stream.write_all(data.as_ref()).await?;
            }
            Crement(val) => self.stream.write_all(val.to_string().as_bytes()).await?,
            ClientError(val) => {
                self.stream.write_all(b"CLIENT_ERROR ").await?;
                self.stream.write_all(val.as_bytes()).await?;
            }
            ServerError(val) => {
                self.stream.write_all(b"SERVER_ERROR ").await?;
                self.stream.write_all(val.as_bytes()).await?;
            }
            Deleted => self.stream.write_all(b"DELETED").await?,
            Stored => self.stream.write_all(b"STORED").await?,
            NotStored => self.stream.write_all(b"NOT_STORED").await?,
            Touched => self.stream.write_all(b"TOUCHED").await?,
            Exists => self.stream.write_all(b"EXISTS").await?,
            NotFound => self.stream.write_all(b"NOT_FOUND").await?,

            Error => self.stream.write_all(b"ERROR").await?,
        }
        // All response end in "\r\n"
        self.stream.write_all(b"\r\n").await?;

        Ok(())
    }

    pub async fn write_and_flush(&mut self, frame: ResponseFrame) -> Result<()> {
        self.write_value(frame).await?;
        self.stream.flush().await?;
        Ok(())
    }

    pub async fn write_and_end(&mut self, frame: ResponseFrame) -> Result<()> {
        self.write_value(frame).await?;
        self.stream.write_all(b"END\r\n").await?;
        self.stream.flush().await?;
        Ok(())
    }

    pub async fn write(&mut self, frame: ResponseFrame) -> Result<()> {
        self.write_value(frame).await?;
        Ok(())
    }

    pub async fn end_and_flush(&mut self) -> Result<()> {
        // Check that all multi response have "END"
        self.stream.write_all(b"END\r\n").await?;
        self.stream.flush().await?;
        Ok(())
    }

    // pub async fn write_frames(&mut self, frames: Vec<ResponseFrame>) -> Result<()> {
    //     for frame in frames {
    //         self.write_value(frame).await?
    //     }
    //     // Check that all multi response have "END"
    //     self.stream.write_all(b"END\r\n").await?;
    //     self.stream.flush().await?;
    //     Ok(())
    // }
}