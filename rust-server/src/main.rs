mod responder;

use responder::ExampleResponder;
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
