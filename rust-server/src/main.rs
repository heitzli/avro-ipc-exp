mod responder;

use apache_avro::types::Value;
use responder::{AvroHandler, AvroResponder, ServerResult};
use rust_avro_rpc::transport::{read_framed_message, write_framed_message};
use std::io::{Read, Write};
use std::net::TcpListener;
use std::sync::Arc;
use std::thread;

#[cfg(unix)]
use std::os::unix::net::UnixListener;

const DEFAULT_TCP_PORT: u16 = 9000;
const DEFAULT_UNIX_SOCKET: &str = "/tmp/example.sock";

fn print_usage(program: &str) {
    eprintln!("Usage: {program} [--tcp-port PORT] [--unix-socket PATH] [--no-unix]");
}

fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let args: Vec<String> = std::env::args().collect();
    let program = args
        .first()
        .cloned()
        .unwrap_or_else(|| "rust-avro-server".into());

    let mut tcp_port = DEFAULT_TCP_PORT;
    let mut unix_socket = DEFAULT_UNIX_SOCKET.to_string();
    let mut no_unix = false;

    let mut index = 1;
    while index < args.len() {
        match args[index].as_str() {
            "--tcp-port" => {
                index += 1;
                tcp_port = args
                    .get(index)
                    .and_then(|value| value.parse().ok())
                    .ok_or_else(|| -> Box<dyn std::error::Error + Send + Sync> {
                        "missing value for --tcp-port".to_string().into()
                    })?;
            }
            "--unix-socket" => {
                index += 1;
                unix_socket = args.get(index).cloned().ok_or_else(
                    || -> Box<dyn std::error::Error + Send + Sync> {
                        "missing value for --unix-socket".to_string().into()
                    },
                )?;
            }
            "--no-unix" => no_unix = true,
            "--help" | "-h" => {
                print_usage(&program);
                return Ok(());
            }
            other => {
                print_usage(&program);
                return Err(format!("unknown argument: {other}").into());
            }
        }
        index += 1;
    }

    let responder = Arc::new(ExampleResponder::new()?);

    /*
     * TCP listener.
     */

    let tcp_listener = TcpListener::bind(("127.0.0.1", tcp_port))?;
    println!("[server] listening on 127.0.0.1:{tcp_port}");

    let tcp_responder = Arc::clone(&responder);
    let tcp_thread = thread::spawn(move || {
        for stream in tcp_listener.incoming() {
            match stream {
                Ok(stream) => {
                    let responder = Arc::clone(&tcp_responder);
                    thread::spawn(move || {
                        let peer = stream
                            .peer_addr()
                            .map(|addr| addr.to_string())
                            .unwrap_or_else(|_| "<unknown>".into());
                        handle_connection(stream, &responder, &peer);
                    });
                }
                Err(error) => eprintln!("[server] TCP accept failed: {error}"),
            }
        }
    });

    /*
     * Unix socket listener.
     */

    #[cfg(unix)]
    let unix_thread = if no_unix {
        None
    } else {
        let _ = std::fs::remove_file(&unix_socket);
        let unix_listener = UnixListener::bind(&unix_socket)?;
        println!("[server] listening on {unix_socket}");

        let unix_responder = Arc::clone(&responder);
        let socket_name = unix_socket.clone();
        Some(thread::spawn(move || {
            for stream in unix_listener.incoming() {
                match stream {
                    Ok(stream) => {
                        let responder = Arc::clone(&unix_responder);
                        let name = socket_name.clone();
                        thread::spawn(move || handle_connection(stream, &responder, &name));
                    }
                    Err(error) => eprintln!("[server] Unix accept failed: {error}"),
                }
            }
        }))
    };

    #[cfg(not(unix))]
    let _ = (&no_unix, &unix_socket);

    println!("[server] Rust Avro RPC server is running");

    tcp_thread
        .join()
        .map_err(|_| -> Box<dyn std::error::Error + Send + Sync> {
            "TCP listener thread failed".into()
        })?;

    #[cfg(unix)]
    if let Some(unix_thread) = unix_thread {
        unix_thread
            .join()
            .map_err(|_| -> Box<dyn std::error::Error + Send + Sync> {
                "Unix listener thread failed".into()
            })?;
    }

    Ok(())
}

/*
 * ==========================================================================
 * Example protocol handler (the only per-protocol code)
 * ==========================================================================
 *
 * To serve a different `.avpr`, add a new `struct MyHandler` implementing
 * `AvroHandler` and use `AvroResponder::from_protocol(text, MyHandler)`.
 */

const PROTOCOL: &str = include_str!("../../protocol/example.avpr");

pub struct ExampleHandler;

impl AvroHandler for ExampleHandler {
    fn invoke(&self, method: &str, params: &Value) -> ServerResult<Value> {
        match method {
            "echo" => ExampleHandler::echo(params),
            "add" => ExampleHandler::add(params),
            "test" => ExampleHandler::test(params),
            _ => Err(format!("unknown RPC method: {method}").into()),
        }
    }
}

pub type ExampleResponder = AvroResponder<ExampleHandler>;

impl ExampleResponder {
    pub fn new() -> ServerResult<Self> {
        Self::with_protocol(PROTOCOL)
    }

    pub fn with_protocol(protocol_text: &str) -> ServerResult<Self> {
        Self::from_protocol(protocol_text, ExampleHandler)
    }
}

impl ExampleHandler {
    fn inner(params: &Value) -> ServerResult<&Vec<(String, Value)>> {
        match params {
            Value::Record(fields) => {
                if fields.len() != 1 {
                    return Err(format!("invalid RPC params wrapper: {params:?}").into());
                }

                match &fields[0].1 {
                    Value::Record(inner) => Ok(inner),
                    other => Err(format!("invalid RPC params record: {other:?}").into()),
                }
            }
            other => Err(format!("invalid RPC params: {other:?}").into()),
        }
    }

    fn field<'a>(fields: &'a [(String, Value)], name: &str) -> ServerResult<&'a Value> {
        fields
            .iter()
            .find(|(field_name, _)| field_name == name)
            .map(|(_, value)| value)
            .ok_or_else(|| format!("request is missing '{name}' field").into())
    }

    fn echo(params: &Value) -> ServerResult<Value> {
        let inner = Self::inner(params)?;

        let message = match Self::field(inner, "message")? {
            Value::String(message) => message.clone(),
            other => return Err(format!("echo field 'message' has invalid type: {other:?}").into()),
        };

        println!("[server] echo({message:?})");

        Ok(Value::Record(vec![(
            "message".to_string(),
            Value::String(message),
        )]))
    }

    fn add(params: &Value) -> ServerResult<Value> {
        let inner = Self::inner(params)?;

        let a = match Self::field(inner, "a")? {
            Value::Long(a) => *a,
            other => return Err(format!("add field 'a' has invalid type: {other:?}").into()),
        };

        let b = match Self::field(inner, "b")? {
            Value::Long(b) => *b,
            other => return Err(format!("add field 'b' has invalid type: {other:?}").into()),
        };

        let result = a + b;

        println!("[server] add({a}, {b}) -> {result}");

        Ok(Value::Record(vec![(
            "result".to_string(),
            Value::Long(result),
        )]))
    }

    fn test(params: &Value) -> ServerResult<Value> {
        let inner = Self::inner(params)?;

        let a = match Self::field(inner, "a")? {
            Value::Int(a) => *a,
            other => return Err(format!("test field 'a' has invalid type: {other:?}").into()),
        };

        let b = match Self::field(inner, "b")? {
            Value::Int(b) => *b,
            other => return Err(format!("test field 'b' has invalid type: {other:?}").into()),
        };

        let result = a - b;

        println!("[server] test({a}, {b}) -> {result}");

        Ok(Value::Record(vec![(
            "result".to_string(),
            Value::Int(result),
        )]))
    }
}

/*
 * ==========================================================================
 * Per-connection request loop
 * ==========================================================================
 */

fn handle_connection<S: Read + Write>(mut stream: S, responder: &ExampleResponder, address: &str) {
    println!("[server] connection from {address}");

    loop {
        let request = match read_framed_message(&mut stream) {
            Ok(request) => request,
            Err(error) => {
                println!("[server] connection closed: {error}");
                break;
            }
        };

        if request.is_empty() {
            break;
        }

        let response = responder.respond(&request);

        if let Err(error) = write_framed_message(&mut stream, &response) {
            println!("[server] write failed: {error}");
            break;
        }
    }

    println!("[server] disconnected: {address}");
}
