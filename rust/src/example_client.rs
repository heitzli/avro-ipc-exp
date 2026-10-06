use apache_avro::types::Value;

use crate::avro_rpc::AvroRpcClient;
use crate::transport::Transport;

type ClientResult<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

const PROTOCOL: &str = include_str!("../../protocol/example.avpr");

pub struct ExampleServiceClient<T: Transport> {
    rpc: AvroRpcClient<T>,
}

impl<T: Transport> ExampleServiceClient<T> {
    /*
     * ======================================================================
     * Create client
     * ======================================================================
     */

    pub fn new(transport: T) -> ClientResult<Self> {
        let rpc = AvroRpcClient::new(transport, PROTOCOL)?;

        Ok(Self { rpc })
    }

    /*
     * ======================================================================
     * echo
     * ======================================================================
     *
     * Developer-facing API:
     *
     *     client.echo("hello")?;
     *
     * Internally this becomes the Avro RPC request:
     *
     *     {
     *         "request": {
     *             "message": "hello"
     *         }
     *     }
     */

    pub fn echo(&mut self, message: &str) -> ClientResult<String> {
        let request = Value::Record(vec![(
            "request".to_string(),
            Value::Record(vec![(
                "message".to_string(),
                Value::String(message.to_string()),
            )]),
        )]);

        let response = self.rpc.call("echo", request)?;

        Self::extract_echo_response(response)
    }

    /*
     * ======================================================================
     * add
     * ======================================================================
     *
     * Developer-facing API:
     *
     *     client.add(10, 20)?;
     *
     * Internally this becomes:
     *
     *     {
     *         "request": {
     *             "a": 10,
     *             "b": 20
     *         }
     *     }
     */

    pub fn add(&mut self, a: i64, b: i64) -> ClientResult<i64> {
        let request = Value::Record(vec![(
            "request".to_string(),
            Value::Record(vec![
                ("a".to_string(), Value::Long(a)),
                ("b".to_string(), Value::Long(b)),
            ]),
        )]);

        let response = self.rpc.call("add", request)?;

        Self::extract_add_response(response)
    }

    /*
     * ======================================================================
     * Extract echo response
     * ======================================================================
     */

    fn extract_echo_response(response: Value) -> ClientResult<String> {
        match response {
            Value::Record(fields) => {
                for (name, value) in fields {
                    if name == "message" {
                        if let Value::String(message) = value {
                            return Ok(message);
                        }

                        return Err("echo response field 'message' has invalid type".into());
                    }
                }

                Err("echo response missing 'message' field".into())
            }

            other => Err(format!("invalid echo response: {other:?}").into()),
        }
    }

    /*
     * ======================================================================
     * Extract add response
     * ======================================================================
     */

    fn extract_add_response(response: Value) -> ClientResult<i64> {
        match response {
            Value::Record(fields) => {
                for (name, value) in fields {
                    if name == "result" {
                        if let Value::Long(result) = value {
                            return Ok(result);
                        }

                        return Err("add response field 'result' has invalid type".into());
                    }
                }

                Err("add response missing 'result' field".into())
            }

            other => Err(format!("invalid add response: {other:?}").into()),
        }
    }

    /*
     * ======================================================================
     * Close
     * ======================================================================
     */

    pub fn close(&mut self) -> ClientResult<()> {
        self.rpc.close()
    }
}
