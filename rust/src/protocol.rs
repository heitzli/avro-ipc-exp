use apache_avro::types::Value;
use apache_avro::{Schema, from_avro_datum, to_avro_datum};

use std::io::Cursor;

// ============================================================
// Method schemas
// ============================================================

const ECHO_REQUEST_SCHEMA: &str = r#"
{
  "type": "record",
  "name": "EchoRequest",
  "namespace": "example",
  "fields": [
    {
      "name": "message",
      "type": "string"
    }
  ]
}
"#;

const ECHO_RESPONSE_SCHEMA: &str = r#"
{
  "type": "record",
  "name": "EchoResponse",
  "namespace": "example",
  "fields": [
    {
      "name": "message",
      "type": "string"
    }
  ]
}
"#;

const ADD_REQUEST_SCHEMA: &str = r#"
{
  "type": "record",
  "name": "AddRequest",
  "namespace": "example",
  "fields": [
    {
      "name": "a",
      "type": "long"
    },
    {
      "name": "b",
      "type": "long"
    }
  ]
}
"#;

const ADD_RESPONSE_SCHEMA: &str = r#"
{
  "type": "record",
  "name": "AddResponse",
  "namespace": "example",
  "fields": [
    {
      "name": "result",
      "type": "long"
    }
  ]
}
"#;

// ============================================================
// RPC envelope schemas
// ============================================================

const REQUEST_ENVELOPE_SCHEMA: &str = r#"
{
  "type": "record",
  "name": "RpcRequest",
  "namespace": "example.rpc",
  "fields": [
    {
      "name": "request_id",
      "type": "long"
    },
    {
      "name": "method",
      "type": "string"
    },
    {
      "name": "payload",
      "type": "bytes"
    }
  ]
}
"#;

const RESPONSE_ENVELOPE_SCHEMA: &str = r#"
{
  "type": "record",
  "name": "RpcResponse",
  "namespace": "example.rpc",
  "fields": [
    {
      "name": "request_id",
      "type": "long"
    },
    {
      "name": "success",
      "type": "boolean"
    },
    {
      "name": "payload",
      "type": "bytes"
    },
    {
      "name": "error",
      "type": ["null", "string"],
      "default": null
    }
  ]
}
"#;

// ============================================================
// Protocol
// ============================================================

pub struct Protocol {
    request_envelope: Schema,
    response_envelope: Schema,

    echo_request: Schema,
    echo_response: Schema,

    add_request: Schema,
    add_response: Schema,
}

impl Protocol {
    pub fn new() -> Result<Self, Box<dyn std::error::Error>> {
        Ok(Self {
            request_envelope: Schema::parse_str(REQUEST_ENVELOPE_SCHEMA)?,

            response_envelope: Schema::parse_str(RESPONSE_ENVELOPE_SCHEMA)?,

            echo_request: Schema::parse_str(ECHO_REQUEST_SCHEMA)?,

            echo_response: Schema::parse_str(ECHO_RESPONSE_SCHEMA)?,

            add_request: Schema::parse_str(ADD_REQUEST_SCHEMA)?,

            add_response: Schema::parse_str(ADD_RESPONSE_SCHEMA)?,
        })
    }

    // ========================================================
    // echo
    // ========================================================

    pub fn encode_echo_request(
        &self,
        request_id: i64,
        message: &str,
    ) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
        let payload = Value::Record(vec![(
            "message".to_string(),
            Value::String(message.to_string()),
        )]);

        let payload = to_avro_datum(&self.echo_request, payload)?;

        let envelope = Value::Record(vec![
            ("request_id".to_string(), Value::Long(request_id)),
            ("method".to_string(), Value::String("echo".to_string())),
            ("payload".to_string(), Value::Bytes(payload)),
        ]);

        Ok(to_avro_datum(&self.request_envelope, envelope)?)
    }

    pub fn decode_echo_response(
        &self,
        payload: &[u8],
    ) -> Result<String, Box<dyn std::error::Error>> {
        let response = self.decode_response(payload)?;

        if !response.success {
            return Err(response
                .error
                .unwrap_or_else(|| "unknown RPC error".to_string())
                .into());
        }

        let value = from_avro_datum(
            &self.echo_response,
            &mut Cursor::new(response.payload),
            None,
        )?;

        match value {
            Value::Record(fields) => {
                for (name, value) in fields {
                    if name == "message" {
                        if let Value::String(message) = value {
                            return Ok(message);
                        }
                    }
                }

                Err("missing message".into())
            }

            _ => Err("invalid EchoResponse".into()),
        }
    }

    // ========================================================
    // add
    // ========================================================

    pub fn encode_add_request(
        &self,
        request_id: i64,
        a: i64,
        b: i64,
    ) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
        let payload = Value::Record(vec![
            ("a".to_string(), Value::Long(a)),
            ("b".to_string(), Value::Long(b)),
        ]);

        let payload = to_avro_datum(&self.add_request, payload)?;

        let envelope = Value::Record(vec![
            ("request_id".to_string(), Value::Long(request_id)),
            ("method".to_string(), Value::String("add".to_string())),
            ("payload".to_string(), Value::Bytes(payload)),
        ]);

        Ok(to_avro_datum(&self.request_envelope, envelope)?)
    }

    pub fn decode_add_response(&self, payload: &[u8]) -> Result<i64, Box<dyn std::error::Error>> {
        let response = self.decode_response(payload)?;

        if !response.success {
            return Err(response
                .error
                .unwrap_or_else(|| "unknown RPC error".to_string())
                .into());
        }

        let value = from_avro_datum(&self.add_response, &mut Cursor::new(response.payload), None)?;

        match value {
            Value::Record(fields) => {
                for (name, value) in fields {
                    if name == "result" {
                        if let Value::Long(result) = value {
                            return Ok(result);
                        }
                    }
                }

                Err("missing result".into())
            }

            _ => Err("invalid AddResponse".into()),
        }
    }

    // ========================================================
    // Response envelope
    // ========================================================

    fn decode_response(&self, data: &[u8]) -> Result<RpcResponse, Box<dyn std::error::Error>> {
        let value = from_avro_datum(&self.response_envelope, &mut Cursor::new(data), None)?;

        match value {
            Value::Record(fields) => {
                let mut request_id = None;
                let mut success = None;
                let mut payload = None;
                let mut error = None;

                for (name, value) in fields {
                    match name.as_str() {
                        "request_id" => {
                            if let Value::Long(v) = value {
                                request_id = Some(v);
                            }
                        }

                        "success" => {
                            if let Value::Boolean(v) = value {
                                success = Some(v);
                            }
                        }

                        "payload" => {
                            if let Value::Bytes(v) = value {
                                payload = Some(v);
                            }
                        }

                        "error" => {
                            if let Value::Union(index, value) = value {
                                if index == 1 {
                                    if let Value::String(message) = *value {
                                        error = Some(message);
                                    }
                                }
                            }
                        }

                        _ => {}
                    }
                }

                Ok(RpcResponse {
                    request_id: request_id.ok_or("missing request_id")?,

                    success: success.ok_or("missing success")?,

                    payload: payload.ok_or("missing payload")?,

                    error,
                })
            }

            _ => Err("invalid RpcResponse".into()),
        }
    }
}

struct RpcResponse {
    request_id: i64,
    success: bool,
    payload: Vec<u8>,
    error: Option<String>,
}
