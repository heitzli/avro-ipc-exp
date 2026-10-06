//! Full RPC protocol-evolution matrix against the Python responder.
//!
//! Requires Python with `avro` installed (uses `../python/.venv/bin/python`
//! when present). Spawns three responders:
//!
//! * v1 on 127.0.0.1:9001 (`protocol/example_v1.avpr`, baseline)
//! * v2 on 127.0.0.1:9002 (`protocol/example_v2.avpr`, `c`/`note` with defaults)
//! * v3_bad on 127.0.0.1:9003 (`protocol/example_v3_bad.avpr`, required `c`)
//!
//! Cases:
//! * A old -> new: v1 client vs v2 server (request default `c` filled server-side)
//! * B new -> old: v2 client vs v1 server (extra `c` ignored; response `note` defaulted client-side)
//! * C negative: v1 client vs v3 server must fail (missing required `c`)

use rust_avro_rpc::example_client::ExampleServiceClient;
use rust_avro_rpc::transport::TcpTransport;
use std::net::TcpStream;
use std::path::PathBuf;
use std::process::{Child, Command};
use std::time::{Duration, Instant};

const V1: &str = include_str!("../../protocol/example_v1.avpr");
const V2: &str = include_str!("../../protocol/example_v2.avpr");

fn workspace_paths() -> (PathBuf, PathBuf) {
    let rust_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let root = rust_dir.parent().unwrap().to_path_buf();
    (root.join("python").join("main.py"), root.join("protocol"))
}

fn python_binary() -> String {
    let rust_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let venv = rust_dir
        .parent()
        .unwrap()
        .join("python")
        .join(".venv")
        .join("bin")
        .join("python");
    if venv.exists() {
        venv.to_string_lossy().into_owned()
    } else {
        "python3".to_string()
    }
}

fn wait_for_port(port: u16) {
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        if TcpStream::connect(("127.0.0.1", port)).is_ok() {
            return;
        }
        if Instant::now() > deadline {
            panic!("server on port {port} did not start in time");
        }
        std::thread::sleep(Duration::from_millis(100));
    }
}

struct TestServers {
    children: Vec<Child>,
}

impl TestServers {
    fn start() -> Self {
        let (server_script, protocol_dir) = workspace_paths();
        let python = python_binary();
        let cases = [
            ("example_v1.avpr", 9001),
            ("example_v2.avpr", 9002),
            ("example_v3_bad.avpr", 9003),
        ];
        let mut children = Vec::new();
        for (file, port) in cases {
            let child = Command::new(&python)
                .arg(&server_script)
                .arg("--protocol")
                .arg(protocol_dir.join(file))
                .arg("--tcp-port")
                .arg(port.to_string())
                .arg("--no-unix")
                .spawn()
                .unwrap_or_else(|e| panic!("failed to spawn python server {file}: {e}"));
            children.push(child);
        }
        for port in [9001, 9002, 9003] {
            wait_for_port(port);
        }
        // Give responders a moment after accepting the probe connection.
        std::thread::sleep(Duration::from_millis(200));
        Self { children }
    }
}

impl Drop for TestServers {
    fn drop(&mut self) {
        for child in &mut self.children {
            let _ = child.kill();
        }
        for child in &mut self.children {
            let _ = child.wait();
        }
    }
}

fn v1_client(port: u16) -> ExampleServiceClient<TcpTransport> {
    let transport = TcpTransport::connect("127.0.0.1", port).expect("connect");
    ExampleServiceClient::with_protocol(transport, V1).expect("parse v1")
}

fn v2_client(port: u16) -> ExampleServiceClient<TcpTransport> {
    let transport = TcpTransport::connect("127.0.0.1", port).expect("connect");
    ExampleServiceClient::with_protocol(transport, V2).expect("parse v2")
}

#[test]
fn rpc_evolution_matrix() {
    let _servers = TestServers::start();

    // C: baseline v1 -> v1 sanity.
    {
        let mut client = v1_client(9001);
        assert_eq!(client.test(10, 3).expect("v1->v1"), 7);
        client.close().ok();
    }

    // A: old -> new. v1 request lacks `c`; v2 server fills default "new".
    // v2 response carries `note`; v1 client drops it via reader resolution.
    {
        let mut client = v1_client(9002);
        let result = client.test(42, 7).expect("v1->v2 must succeed");
        assert_eq!(result, 35);
        assert!(
            client.has_peer_protocol(),
            "client should have learned the v2 server protocol via handshake"
        );
        assert_ne!(
            client.last_handshake_match(),
            Some("NONE"),
            "handshake retry must have resolved, got {:?}",
            client.last_handshake_match()
        );
        client.close().ok();
    }

    // B: new -> old. v2 request carries `c`; v1 server ignores it.
    // v1 response lacks `note`; v2 client fills default "n/a".
    {
        let mut client = v2_client(9001);
        let (result, note) = client.test_v2(42, 7, "hi").expect("v2->v1 must succeed");
        assert_eq!(result, 35);
        assert_eq!(
            note.as_deref(),
            Some("n/a"),
            "v2 client must default `note` when reading a v1 response, got {note:?}"
        );
        client.close().ok();
    }

    // D: negative. v1 request lacks required `c`; v3 server must error.
    {
        let mut client = v1_client(9003);
        let err = client
            .test(1, 2)
            .expect_err("v1->v3 (required c, no default) must fail");
        let msg = err.to_string().to_lowercase();
        assert!(
            msg.contains("remote")
                || msg.contains("default")
                || msg.contains('c')
                || msg.contains("resolution"),
            "expected remote/resolution error, got: {err}"
        );
        client.close().ok();
    }
}
