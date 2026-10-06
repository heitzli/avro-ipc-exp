use apache_avro::types::Value;

use crate::avro_rpc::AvroRpcClient;
use crate::transport::Transport;

type ClientResult<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

const PROTOCOL: &str = include_str!("../../protocol/example.avpr");

// This is a handwritten stub for the avro protocol, for the moment ignore code gen and creat such clients by hand.

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
        Self::with_protocol(transport, PROTOCOL)
    }

    pub fn with_protocol(transport: T, protocol_text: &str) -> ClientResult<Self> {
        let rpc = AvroRpcClient::new(transport, protocol_text)?;

        Ok(Self { rpc })
    }

    pub fn last_handshake_match(&self) -> Option<&str> {
        self.rpc.last_handshake_match()
    }

    pub fn has_peer_protocol(&self) -> bool {
        self.rpc.has_peer_protocol()
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

    pub fn test(&mut self, a: i32, b: i32) -> ClientResult<i32> {
        let request = Value::Record(vec![(
            "request".to_string(),
            Value::Record(vec![
                ("a".to_string(), Value::Int(a)),
                ("b".to_string(), Value::Int(b)),
            ]),
        )]);

        let response = self.rpc.call("test", request)?;

        Self::extract_test_response(response)
    }

    /// v2 request including evolved field `c`.
    pub fn test_v2(&mut self, a: i32, b: i32, c: &str) -> ClientResult<(i32, Option<String>)> {
        let request = Value::Record(vec![(
            "request".to_string(),
            Value::Record(vec![
                ("a".to_string(), Value::Int(a)),
                ("b".to_string(), Value::Int(b)),
                ("c".to_string(), Value::String(c.to_string())),
            ]),
        )]);

        let response = self.rpc.call("test", request)?;

        Self::extract_test_response_with_note(response)
    }

    /// Low-level escape hatch for evolution tests (partial records, etc.).
    pub fn call_raw(&mut self, method: &str, request: Value) -> ClientResult<Value> {
        self.rpc.call(method, request)
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

    fn extract_test_response(response: Value) -> ClientResult<i32> {
        Self::extract_test_response_with_note(response).map(|(result, _)| result)
    }

    fn extract_test_response_with_note(response: Value) -> ClientResult<(i32, Option<String>)> {
        match response {
            Value::Record(fields) => {
                let mut result = None;
                let mut note = None;

                for (name, value) in fields {
                    match name.as_str() {
                        "result" => {
                            if let Value::Int(v) = value {
                                result = Some(v);
                            } else {
                                return Err("test response field 'result' has invalid type".into());
                            }
                        }
                        "note" => {
                            if let Value::String(v) = value {
                                note = Some(v);
                            } else {
                                return Err("test response field 'note' has invalid type".into());
                            }
                        }
                        _ => {}
                    }
                }

                match result {
                    Some(result) => Ok((result, note)),
                    None => Err("test response missing 'result' field".into()),
                }
            }

            other => Err(format!("invalid test response: {other:?}").into()),
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
