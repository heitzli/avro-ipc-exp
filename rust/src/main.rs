use rust_avro_rpc::example_client::ExampleServiceClient;
use rust_avro_rpc::transport::TcpTransport;
#[cfg(unix)]
use rust_avro_rpc::transport::UnixTransport;

fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    println!("=== Avro RPC Rust client ===");

    // Unix socket example
    #[cfg(unix)]
    {
        println!();
        println!("Connecting through Unix socket: /tmp/example.sock");

        let transport = UnixTransport::connect("/tmp/example.sock")?;
        let mut client = ExampleServiceClient::new(transport)?;

        let message = client.echo("hello from Rust")?;
        println!("echo -> {message}");

        let result = client.add(10, 20)?;
        println!("add -> {result}");

        let res = client.test(42, 42)?;
        println!("test -> {res}");

        client.close()?;
    }

    // TCP example
    println!();
    println!("Connecting through TCP: 127.0.0.1:9000");

    let transport = TcpTransport::connect("127.0.0.1", 9000)?;
    let mut client = ExampleServiceClient::new(transport)?;

    let message = client.echo("hello over TCP from Rust")?;
    println!("echo -> {message}");

    let result = client.add(100, 200)?;
    println!("add -> {result}");

    client.close()?;

    Ok(())
}
