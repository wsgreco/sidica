mod get;
mod set;

use crate::{cache::Cache, frame::RequestFrame, parse::Parse, Connection};
use anyhow::Result;
pub use get::Get;
pub use set::Set;
use thiserror::Error;

#[derive(Error, Debug, PartialEq)]
pub(crate) enum CommandError {
    #[error("command error; unknown command")]
    Unknown,
}

#[derive(Debug)]
pub enum Command {
    Get(Get),
    Set(Set),
}

impl Command {
    /// Parse a command from a received frame.
    ///
    /// The `Frame` must represent a Redis command supported by `mini-redis` and
    /// be the array variant.
    ///
    /// # Returns
    ///
    /// On success, the command value is returned, otherwise, `Err` is returned.
    pub fn from_frame(frame: RequestFrame) -> Result<Command> {
        let command = match frame {
            RequestFrame::Other(frame) => {
                let mut parse = Parse::new(frame);
                let command_name = parse.next_string()?;
                let c = match &command_name[..] {
                    "get" => Command::Get(Get::parse_frame(&mut parse)?),
                    _ => {
                        // Return `Unknown` to skip the `finish()` call. As
                        // the command is not recognized, there will likely
                        // be fields remaining in the `Parse` instance.
                        return Err(CommandError::Unknown.into());
                    }
                };
                parse.finish()?;
                c
            }
            RequestFrame::Storage(frame) => {
                let mut parse = Parse::new(frame.command_line);
                let command_name = parse.next_string()?;

                let c = match &command_name[..] {
                    "set" => Command::Set(Set::parse_frame(&mut parse, frame.data)?),
                    _ => {
                        // Return `Unknown` to skip the `finish()` call. As
                        // the command is not recognized, there will likely
                        // be fields remaining in the `Parse` instance.
                        return Err(CommandError::Unknown.into());
                    }
                };
                parse.finish()?;
                c
            }
        };

        // Check if there is any remaining unconsumed fields in the `Parse`
        // value. If fields remain, this indicates an unexpected frame format
        // and an error is returned.
        // parse.finish()?;

        // The command has been successfully parsed
        Ok(command)
    }

    /// Apply the command to the specified `Cache` instance.
    ///
    /// The response is written to `dst`. This is called by the server in order
    /// to execute a received command.
    pub(crate) async fn apply(
        self,
        cache: Cache,
        dst: &mut Connection,
        // shutdown: &mut Shutdown,
    ) -> Result<()> {
        match self {
            Command::Get(cmd) => cmd.apply(cache, dst).await,
            Command::Set(cmd) => cmd.apply(cache, dst).await,
        }
    }

    /// Returns the command name
    pub(crate) fn get_name(&self) -> &str {
        match self {
            Command::Get(_) => "get",
            Command::Set(_) => "set",
        }
    }
}
