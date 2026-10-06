use std::io::{Read, Write};
use std::net::TcpStream;

#[cfg(unix)]
use std::os::unix::net::UnixStream;

pub type TransportResult<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

pub trait Transport {
    fn send(&mut self, message: &[u8]) -> TransportResult<()>;

    fn receive(&mut self) -> TransportResult<Vec<u8>>;

    fn close(&mut self) -> TransportResult<()>;
}

/*
 * ==========================================================================
 * TCP transport
 * ==========================================================================
 */

pub struct TcpTransport {
    stream: TcpStream,
}

impl TcpTransport {
    pub fn connect(host: &str, port: u16) -> TransportResult<Self> {
        let stream = TcpStream::connect((host, port))?;

        stream.set_nodelay(true)?;

        Ok(Self { stream })
    }
}

impl Transport for TcpTransport {
    fn send(&mut self, message: &[u8]) -> TransportResult<()> {
        write_framed_message(&mut self.stream, message)?;

        Ok(())
    }

    fn receive(&mut self) -> TransportResult<Vec<u8>> {
        Ok(read_framed_message(&mut self.stream)?)
    }

    fn close(&mut self) -> TransportResult<()> {
        self.stream.shutdown(std::net::Shutdown::Both)?;

        Ok(())
    }
}

/*
 * ==========================================================================
 * Unix socket transport
 * ==========================================================================
 */

#[cfg(unix)]
pub struct UnixTransport {
    stream: UnixStream,
}

#[cfg(unix)]
impl UnixTransport {
    pub fn connect(path: &str) -> TransportResult<Self> {
        let stream = UnixStream::connect(path)?;

        Ok(Self { stream })
    }
}

#[cfg(unix)]
impl Transport for UnixTransport {
    fn send(&mut self, message: &[u8]) -> TransportResult<()> {
        write_framed_message(&mut self.stream, message)?;

        Ok(())
    }

    fn receive(&mut self) -> TransportResult<Vec<u8>> {
        Ok(read_framed_message(&mut self.stream)?)
    }

    fn close(&mut self) -> TransportResult<()> {
        self.stream.shutdown(std::net::Shutdown::Both)?;

        Ok(())
    }
}

/*
 * ==========================================================================
 * Avro framing
 * ==========================================================================
 *
 * An Avro message consists of:
 *
 *   4-byte big-endian length
 *   data
 *   4-byte zero terminator
 *
 * Multiple buffers can exist in one message. For our implementation we
 * currently send the complete message as one buffer.
 */

fn write_framed_message<W: Write>(writer: &mut W, message: &[u8]) -> TransportResult<()> {
    let length = u32::try_from(message.len())?;

    writer.write_all(&length.to_be_bytes())?;

    writer.write_all(message)?;

    writer.write_all(&0u32.to_be_bytes())?;

    writer.flush()?;

    Ok(())
}

fn read_framed_message<R: Read>(reader: &mut R) -> TransportResult<Vec<u8>> {
    let mut result = Vec::new();

    loop {
        let mut length_bytes = [0u8; 4];

        reader.read_exact(&mut length_bytes)?;

        let length = u32::from_be_bytes(length_bytes);

        if length == 0 {
            break;
        }

        let length = usize::try_from(length)?;

        let mut buffer = vec![0u8; length];

        reader.read_exact(&mut buffer)?;

        result.extend_from_slice(&buffer);
    }

    Ok(result)
}
