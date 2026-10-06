//! Rust client (`rust-avro-rpc`) against the Rust server.
//!
//! Spawns `rust-avro-server` on TCP 9011 + a scratch Unix socket, then
//! exercises echo/add/test over both transports plus the unknown-method
//! error path.

use apache_avro::types::Value;
use rust_avro_rpc::example_client::ExampleServiceClient;
use rust_avro_rpc::transport::TcpTransport;
use std::net::TcpStream;
use std::process::{Child, Command};
use std::time::{Duration, Instant};

#[cfg(unix)]
use rust_avro_rpc::transport::UnixTransport;

const TCP_PORT: u16 = 9011;
const TCP_PORT_UNIX_TEST: u16 = 9012;

#[cfg(unix)]
const UNIX_SOCKET: &str = "/tmp/example-rs-test.sock";

fn wait_for_tcp(port: u16) {
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        if TcpStream::connect(("127.0.0.1", port)).is_ok() {
            return;
        }
        if Instant::now() > deadline {
            panic!("rust server on port {port} did not start in time");
        }
        std::thread::sleep(Duration::from_millis(100));
    }
}

#[cfg(unix)]
fn wait_for_unix(path: &str) {
    use std::os::unix::net::UnixStream;
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        if UnixStream::connect(path).is_ok() {
            return;
        }
        if Instant::now() > deadline {
            panic!("rust server on socket {path} did not start in time");
        }
        std::thread::sleep(Duration::from_millis(100));
    }
}

struct TestServer {
    child: Child,
}

impl TestServer {
    /// Each test uses its own ports/paths: tests run in parallel and must
    /// not share a listener (a finished test kills its server on drop).
    fn start(tcp_port: u16, unix_socket: Option<&str>) -> Self {
        let server_bin = env!("CARGO_BIN_EXE_rust-avro-server");
        let mut command = Command::new(server_bin);
        command.arg("--tcp-port").arg(tcp_port.to_string());

        match unix_socket {
            Some(path) => {
                command.arg("--unix-socket").arg(path);
            }
            None => {
                command.arg("--no-unix");
            }
        }

        let child = command.spawn().expect("failed to spawn rust-avro-server");
        wait_for_tcp(tcp_port);
        #[cfg(unix)]
        if let Some(path) = unix_socket {
            wait_for_unix(path);
        }
        // Let the server settle after the probe connections.
        std::thread::sleep(Duration::from_millis(200));
        Self { child }
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

#[test]
fn rust_client_against_rust_server_tcp() {
    let _server = TestServer::start(TCP_PORT, None);

    let transport = TcpTransport::connect("127.0.0.1", TCP_PORT).expect("connect");
    let mut client = ExampleServiceClient::new(transport).expect("client");

    assert_eq!(
        client.echo("hello rust-server").expect("echo"),
        "hello rust-server"
    );
    assert_eq!(client.add(100, 200).expect("add"), 300);
    assert_eq!(client.test(42, 7).expect("test"), 35);

    // Same raw protocol bytes on both sides, so the first handshake matches.
    assert_eq!(client.last_handshake_match(), Some("BOTH"));

    // New (v2) client against the v1 server: extra `c` is ignored by the
    // server's reader, mirroring the Python evolution matrix.
    let v2_protocol = include_str!("../../protocol/example_v2.avpr");
    let transport = TcpTransport::connect("127.0.0.1", TCP_PORT).expect("connect");
    let mut v2_client =
        ExampleServiceClient::with_protocol(transport, v2_protocol).expect("client");
    let (result, _) = v2_client.test_v2(42, 7, "hi").expect("v2 test");
    assert_eq!(result, 35);
    v2_client.close().ok();

    // Unknown methods must come back as remote errors, not dropped connections.
    let error = client
        .call_raw("nope", Value::Record(Vec::new()))
        .expect_err("unknown method must fail");
    assert!(
        error.to_string().to_lowercase().contains("unknown"),
        "expected unknown-method error, got: {error}"
    );

    // The connection must still be usable afterwards.
    assert_eq!(client.echo("again").expect("echo after error"), "again");

    client.close().ok();
}

#[cfg(unix)]
#[test]
fn rust_client_against_rust_server_unix() {
    let _server = TestServer::start(TCP_PORT_UNIX_TEST, Some(UNIX_SOCKET));

    let transport = UnixTransport::connect(UNIX_SOCKET).expect("connect");
    let mut client = ExampleServiceClient::new(transport).expect("client");

    assert_eq!(client.echo("hello unix").expect("echo"), "hello unix");
    assert_eq!(client.add(10, 20).expect("add"), 30);
    assert_eq!(client.test(42, 42).expect("test"), 0);

    client.close().ok();
}
