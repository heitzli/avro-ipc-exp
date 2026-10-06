use apache_avro::{
    Schema, from_avro_datum, from_avro_datum_schemata, to_avro_datum, to_avro_datum_schemata,
};

use apache_avro::types::Value;

use md5::{Digest, Md5};

use serde_json::Value as JsonValue;

use std::collections::HashMap;
use std::io::Cursor;

use crate::transport::Transport;

type RpcResult<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

/*
 * ==========================================================================
 * Avro RPC handshake request schema
 * ==========================================================================
 */

const HANDSHAKE_REQUEST_SCHEMA_JSON: &str = r#"
{
  "type": "record",
  "name": "HandshakeRequest",
  "namespace": "org.apache.avro.ipc",

  "fields": [
    {
      "name": "clientHash",
      "type": {
        "type": "fixed",
        "name": "MD5",
        "size": 16
      }
    },

    {
      "name": "clientProtocol",
      "type": [
        "null",
        "string"
      ]
    },

    {
      "name": "serverHash",
      "type": "MD5"
    },

    {
      "name": "meta",
      "type": [
        "null",
        {
          "type": "map",
          "values": "bytes"
        }
      ],
      "default": null
    }
  ]
}
"#;

/*
 * ==========================================================================
 * Avro RPC handshake response schema
 * ==========================================================================
 */

const HANDSHAKE_RESPONSE_SCHEMA_JSON: &str = r#"
{
  "type": "record",
  "name": "HandshakeResponse",
  "namespace": "org.apache.avro.ipc",

  "fields": [
    {
      "name": "match",
      "type": {
        "type": "enum",
        "name": "HandshakeMatch",
        "symbols": [
          "BOTH",
          "CLIENT",
          "NONE"
        ]
      }
    },

    {
      "name": "serverProtocol",
      "type": [
        "null",
        "string"
      ],
      "default": null
    },

    {
      "name": "serverHash",
      "type": [
        "null",
        {
          "type": "fixed",
          "name": "MD5",
          "size": 16
        }
      ],
      "default": null
    },

    {
      "name": "meta",
      "type": [
        "null",
        {
          "type": "map",
          "values": "bytes"
        }
      ],
      "default": null
    }
  ]
}
"#;

/*
 * ==========================================================================
 * RPC metadata schema
 * ==========================================================================
 */

const MAP_BYTES_SCHEMA_JSON: &str = r#"
{
  "type": "map",
  "values": "bytes"
}
"#;

/*
 * ==========================================================================
 * RPC message definition
 * ==========================================================================
 */

#[derive(Debug, Clone)]
struct AvroMessage {
    request_schema: Schema,
    response_schema: Schema,
}

/*
 * ==========================================================================
 * Avro protocol
 * ==========================================================================
 */

pub struct AvroProtocol {
    /*
     * Exact protocol JSON text.
     */
    pub raw: String,

    /*
     * MD5 of the exact protocol bytes.
     */
    pub md5: [u8; 16],

    /*
     * Named schemas from the protocol.
     */
    schemata: Vec<Schema>,

    /*
     * RPC method definitions.
     */
    messages: HashMap<String, AvroMessage>,
}

impl AvroProtocol {
    /*
     * ----------------------------------------------------------------------
     * Parse protocol
     * ----------------------------------------------------------------------
     */

    pub fn parse(raw: &str) -> RpcResult<Self> {
        let protocol: JsonValue = serde_json::from_str(raw)?;

        /*
         * --------------------------------------------------------------
         * Parse named protocol types
         * --------------------------------------------------------------
         */

        let types = protocol
            .get("types")
            .and_then(JsonValue::as_array)
            .ok_or("protocol is missing 'types'")?;

        let type_strings: Vec<String> = types
            .iter()
            .map(serde_json::to_string)
            .collect::<Result<_, _>>()?;

        /*
         * Parse all named types together.
         *
         * This makes references such as:
         *
         *   example.AddRequest
         *
         * available to method schemas.
         */

        let schemata = Schema::parse_list(type_strings.iter().map(String::as_str))?;

        /*
         * --------------------------------------------------------------
         * Parse RPC messages
         * --------------------------------------------------------------
         */

        let messages = protocol
            .get("messages")
            .and_then(JsonValue::as_object)
            .ok_or("protocol is missing 'messages'")?;

        let mut parsed_messages = HashMap::new();

        for (method_name, message) in messages {
            /*
             * ----------------------------------------------------------
             * Request
             * ----------------------------------------------------------
             */

            let request = message
                .get("request")
                .and_then(JsonValue::as_array)
                .ok_or_else(|| format!("method '{method_name}' is missing request"))?;

            /*
             * Avro RPC method parameters are represented as an
             * anonymous record.
             */

            let request_schema_json = serde_json::json!({
                "type": "record",
                "name": format!(
                    "RpcRequest_{}",
                    method_name
                ),
                "namespace": "example",
                "fields": request
            });

            let request_schema_text = serde_json::to_string(&request_schema_json)?;

            let (request_schema, _) = Schema::parse_str_with_list(
                &request_schema_text,
                type_strings.iter().map(String::as_str),
            )?;

            /*
             * ----------------------------------------------------------
             * Response
             * ----------------------------------------------------------
             */

            let response_json = message
                .get("response")
                .ok_or_else(|| format!("method '{method_name}' is missing response"))?;

            let response_schema_text = serde_json::to_string(response_json)?;

            let (response_schema, _) = Schema::parse_str_with_list(
                &response_schema_text,
                type_strings.iter().map(String::as_str),
            )?;

            parsed_messages.insert(
                method_name.clone(),
                AvroMessage {
                    request_schema,
                    response_schema,
                },
            );
        }

        /*
         * --------------------------------------------------------------
         * Calculate protocol MD5
         * --------------------------------------------------------------
         */

        let mut hasher = Md5::new();

        hasher.update(raw.as_bytes());

        let digest = hasher.finalize();

        let mut md5 = [0u8; 16];

        md5.copy_from_slice(&digest);

        Ok(Self {
            raw: raw.to_string(),
            md5,
            schemata,
            messages: parsed_messages,
        })
    }

    /*
     * ----------------------------------------------------------------------
     * Find method
     * ----------------------------------------------------------------------
     */

    fn message(&self, method: &str) -> RpcResult<&AvroMessage> {
        self.messages
            .get(method)
            .ok_or_else(|| format!("unknown RPC method: {method}").into())
    }

    /*
     * ----------------------------------------------------------------------
     * Schema references
     * ----------------------------------------------------------------------
     */

    fn schemata_refs(&self) -> Vec<&Schema> {
        self.schemata.iter().collect()
    }
}

/*
 * ==========================================================================
 * Generic Avro RPC client
 * ==========================================================================
 */

pub struct AvroRpcClient<T: Transport> {
    transport: T,

    protocol: AvroProtocol,

    /*
     * Server protocol hash.
     */
    server_hash: [u8; 16],

    /*
     * Whether at least one successful handshake has happened.
     *
     * We still send a handshake on every request because the Python
     * avro.ipc.Responder used by this project expects one for every
     * responder.respond() call.
     */
    handshake_complete: bool,
}

impl<T: Transport> AvroRpcClient<T> {
    /*
     * ----------------------------------------------------------------------
     * Create client
     * ----------------------------------------------------------------------
     */

    pub fn new(transport: T, protocol_text: &str) -> RpcResult<Self> {
        let protocol = AvroProtocol::parse(protocol_text)?;

        /*
         * Initially assume the server has the same protocol.
         */

        let server_hash = protocol.md5;

        Ok(Self {
            transport,
            protocol,
            server_hash,
            handshake_complete: false,
        })
    }

    /*
     * =========================================================================
     * Close transport
     * =========================================================================
     */

    pub fn close(&mut self) -> RpcResult<()> {
        self.transport.close()
    }

    /*
     * =========================================================================
     * Generic RPC call
     * =========================================================================
     *
     * This is the main public API.
     *
     * The client has no knowledge of individual methods.
     *
     * Example:
     *
     *   let response =
     *       client.call("echo", request)?;
     *
     * or:
     *
     *   let response =
     *       client.call("add", request)?;
     * =========================================================================
     */

    pub fn call(&mut self, method: &str, request: Value) -> RpcResult<Value> {
        /*
         * Get the method definition from the .avpr.
         */

        let message = self.protocol.message(method)?;

        let request_schema = message.request_schema.clone();

        let response_schema = message.response_schema.clone();

        /*
         * --------------------------------------------------------------
         * Encode CallRequest
         * --------------------------------------------------------------
         */

        let request_bytes = self.encode_call_request(method, request, &request_schema)?;

        /*
         * --------------------------------------------------------------
         * Encode handshake
         * --------------------------------------------------------------
         *
         * First request:
         *
         *   clientProtocol = full protocol
         *
         * Subsequent request:
         *
         *   clientProtocol = null
         *
         * If the server responds NONE, we retry with the full protocol.
         */

        let handshake = self.encode_handshake_request(!self.handshake_complete)?;

        /*
         * --------------------------------------------------------------
         * Handshake + CallRequest
         * --------------------------------------------------------------
         *
         * These MUST be inside the same Avro framed message.
         */

        let mut combined = Vec::with_capacity(handshake.len() + request_bytes.len());

        combined.extend_from_slice(&handshake);

        combined.extend_from_slice(&request_bytes);

        /*
         * --------------------------------------------------------------
         * Send
         * --------------------------------------------------------------
         */

        self.transport.send(&combined)?;

        /*
         * --------------------------------------------------------------
         * Receive
         * --------------------------------------------------------------
         *
         * The response contains:
         *
         *   HandshakeResponse
         *   CallResponse
         *
         * in one framed message.
         */

        let response = self.transport.receive()?;

        let mut cursor = Cursor::new(response.as_slice());

        /*
         * --------------------------------------------------------------
         * Decode handshake response
         * --------------------------------------------------------------
         */

        let handshake_response = self.decode_handshake_response(&mut cursor)?;

        /*
         * --------------------------------------------------------------
         * Process handshake
         * --------------------------------------------------------------
         */

        match handshake_response.match_type.as_str() {
            /*
             * ==========================================================
             * BOTH
             *
             * Client and server have the same protocol.
             * ==========================================================
             */
            "BOTH" => {
                if let Some(hash) = handshake_response.server_hash {
                    self.server_hash = hash;
                }

                self.handshake_complete = true;

                self.decode_remaining_response(&response, &cursor, method, &response_schema)
            }

            /*
             * ==========================================================
             * CLIENT
             *
             * Server knows our protocol, but may be using a different
             * server protocol hash.
             * ==========================================================
             */
            "CLIENT" => {
                if let Some(hash) = handshake_response.server_hash {
                    self.server_hash = hash;
                }

                self.handshake_complete = true;

                self.decode_remaining_response(&response, &cursor, method, &response_schema)
            }

            /*
             * ==========================================================
             * NONE
             *
             * Server does not know our client protocol.
             *
             * Retry the SAME RPC call with the complete protocol text.
             * ==========================================================
             */
            "NONE" => {
                if let Some(hash) = handshake_response.server_hash {
                    self.server_hash = hash;
                }

                /*
                 * Build handshake containing the complete protocol.
                 */

                let handshake = self.encode_handshake_request(true)?;

                let mut combined = Vec::with_capacity(handshake.len() + request_bytes.len());

                combined.extend_from_slice(&handshake);

                combined.extend_from_slice(&request_bytes);

                /*
                 * Send retry.
                 */

                self.transport.send(&combined)?;

                /*
                 * Receive retry response.
                 */

                let response = self.transport.receive()?;

                let mut cursor = Cursor::new(response.as_slice());

                let retry_handshake = self.decode_handshake_response(&mut cursor)?;

                match retry_handshake.match_type.as_str() {
                    "BOTH" | "CLIENT" => {
                        if let Some(hash) = retry_handshake.server_hash {
                            self.server_hash = hash;
                        }

                        self.handshake_complete = true;

                        self.decode_remaining_response(&response, &cursor, method, &response_schema)
                    }

                    other => Err(format!("handshake retry failed: {other}").into()),
                }
            }

            other => Err(format!("unknown handshake result: {other}").into()),
        }
    }

    /*
     * =========================================================================
     * Encode handshake request
     * =========================================================================
     */

    fn encode_handshake_request(&self, include_protocol: bool) -> RpcResult<Vec<u8>> {
        let schema = Schema::parse_str(HANDSHAKE_REQUEST_SCHEMA_JSON)?;

        /*
         * clientProtocol:
         *
         *   branch 0 = null
         *   branch 1 = string
         */

        let client_protocol = if include_protocol {
            Value::Union(1, Box::new(Value::String(self.protocol.raw.clone())))
        } else {
            Value::Union(0, Box::new(Value::Null))
        };

        let value = Value::Record(vec![
            (
                "clientHash".to_string(),
                Value::Fixed(16, self.protocol.md5.to_vec()),
            ),
            ("clientProtocol".to_string(), client_protocol),
            (
                "serverHash".to_string(),
                Value::Fixed(16, self.server_hash.to_vec()),
            ),
            ("meta".to_string(), Value::Union(0, Box::new(Value::Null))),
        ]);

        Ok(to_avro_datum(&schema, value)?)
    }

    /*
     * =========================================================================
     * Decode handshake response
     * =========================================================================
     */

    fn decode_handshake_response(
        &self,
        cursor: &mut Cursor<&[u8]>,
    ) -> RpcResult<HandshakeResponse> {
        let schema = Schema::parse_str(HANDSHAKE_RESPONSE_SCHEMA_JSON)?;

        let value = from_avro_datum(&schema, cursor, None)?;

        let fields = match value {
            Value::Record(fields) => fields,

            other => {
                return Err(format!("invalid handshake response: {other:?}").into());
            }
        };

        let mut match_type = None;

        let mut server_hash = None;

        for (name, value) in fields {
            match name.as_str() {
                /*
                 * ------------------------------------------------------
                 * match
                 * ------------------------------------------------------
                 */
                "match" => {
                    if let Value::Enum(_, symbol) = value {
                        match_type = Some(symbol);
                    }
                }

                /*
                 * ------------------------------------------------------
                 * serverHash
                 * ------------------------------------------------------
                 */
                "serverHash" => {
                    if let Value::Union(_, boxed) = value {
                        if let Value::Fixed(_, bytes) = *boxed {
                            if bytes.len() != 16 {
                                return Err("serverHash must contain 16 bytes".into());
                            }

                            let mut hash = [0u8; 16];

                            hash.copy_from_slice(&bytes);

                            server_hash = Some(hash);
                        }
                    }
                }

                _ => {}
            }
        }

        let match_type = match_type.ok_or("handshake response missing match")?;

        Ok(HandshakeResponse {
            match_type,
            server_hash,
        })
    }

    /*
     * =========================================================================
     * Decode the CallResponse after the handshake
     * =========================================================================
     */

    fn decode_remaining_response(
        &self,
        response: &[u8],
        cursor: &Cursor<&[u8]>,
        method: &str,
        response_schema: &Schema,
    ) -> RpcResult<Value> {
        let position = usize::try_from(cursor.position())?;

        let remaining = &response[position..];

        if remaining.is_empty() {
            return Err("handshake succeeded but server returned no RPC response".into());
        }

        self.decode_call_response(remaining, method, response_schema)
    }

    /*
     * =========================================================================
     * Encode CallRequest
     * =========================================================================
     */

    fn encode_call_request(
        &self,
        method: &str,
        request: Value,
        request_schema: &Schema,
    ) -> RpcResult<Vec<u8>> {
        /*
         * Avro RPC CallRequest:
         *
         *   metadata
         *   method
         *   parameters
         */

        /*
         * --------------------------------------------------------------
         * Metadata
         * --------------------------------------------------------------
         */

        let metadata_schema = Schema::parse_str(MAP_BYTES_SCHEMA_JSON)?;

        let metadata = to_avro_datum(&metadata_schema, Value::Map(HashMap::new()))?;

        /*
         * --------------------------------------------------------------
         * Method name
         * --------------------------------------------------------------
         */

        let method_bytes = to_avro_datum(&Schema::String, Value::String(method.to_string()))?;

        /*
         * --------------------------------------------------------------
         * Parameters
         * --------------------------------------------------------------
         */

        let request_bytes =
            to_avro_datum_schemata(request_schema, self.protocol.schemata_refs(), request)?;

        /*
         * --------------------------------------------------------------
         * Combine
         * --------------------------------------------------------------
         */

        let mut result =
            Vec::with_capacity(metadata.len() + method_bytes.len() + request_bytes.len());

        result.extend_from_slice(&metadata);

        result.extend_from_slice(&method_bytes);

        result.extend_from_slice(&request_bytes);

        Ok(result)
    }

    /*
     * =========================================================================
     * Decode CallResponse
     * =========================================================================
     */

    fn decode_call_response(
        &self,
        bytes: &[u8],
        method: &str,
        response_schema: &Schema,
    ) -> RpcResult<Value> {
        let mut cursor = Cursor::new(bytes);

        /*
         * --------------------------------------------------------------
         * Metadata
         * --------------------------------------------------------------
         */

        let metadata_schema = Schema::parse_str(MAP_BYTES_SCHEMA_JSON)?;

        let _metadata = from_avro_datum(&metadata_schema, &mut cursor, None)?;

        /*
         * --------------------------------------------------------------
         * Error flag
         * --------------------------------------------------------------
         */

        let error_value = from_avro_datum(&Schema::Boolean, &mut cursor, None)?;

        let is_error = match error_value {
            Value::Boolean(value) => value,

            other => {
                return Err(format!("invalid Avro RPC error flag: {other:?}").into());
            }
        };

        /*
         * --------------------------------------------------------------
         * Remote error
         * --------------------------------------------------------------
         */

        if is_error {
            let error = from_avro_datum(&Schema::String, &mut cursor, None)?;

            return Err(format!("remote Avro RPC error from '{method}': {error:?}").into());
        }

        /*
         * --------------------------------------------------------------
         * Successful response
         * --------------------------------------------------------------
         */

        let result = from_avro_datum_schemata(
            response_schema,
            self.protocol.schemata_refs(),
            &mut cursor,
            None,
        )?;

        Ok(result)
    }
}

/*
 * ==========================================================================
 * Handshake response
 * ==========================================================================
 */

#[derive(Debug)]
struct HandshakeResponse {
    match_type: String,

    server_hash: Option<[u8; 16]>,
}
