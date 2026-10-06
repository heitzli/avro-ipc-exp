use crate::transport::Transport;
use apache_avro::Schema;
use apache_avro::reader::datum::GenericDatumReader;
use apache_avro::types::Value;
use apache_avro::writer::datum::GenericDatumWriter;
use md5::{Digest, Md5};
use serde_json::Value as JsonValue;
use std::collections::HashMap;
use std::io::Cursor;

type RpcResult<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

/*
 * ==========================================================================
 * Avro RPC handshake request schema
 * ==========================================================================
 */

pub const HANDSHAKE_REQUEST_SCHEMA_JSON: &str = r#"
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

pub const HANDSHAKE_RESPONSE_SCHEMA_JSON: &str = r#"
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

pub const MAP_BYTES_SCHEMA_JSON: &str = r#"
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

#[derive(Debug, Clone)]
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
         * Protocol namespace
         * --------------------------------------------------------------
         */

        let namespace = protocol
            .get("namespace")
            .and_then(JsonValue::as_str)
            .unwrap_or("");

        /*
         * --------------------------------------------------------------
         * Parse named protocol types
         * --------------------------------------------------------------
         */

        let types = protocol
            .get("types")
            .and_then(JsonValue::as_array)
            .ok_or("protocol is missing 'types'")?;

        /*
         * The generated Avro protocol puts the namespace on the
         * protocol itself, not on each individual type.
         *
         * Example:
         *
         *   {
         *     "namespace": "example",
         *     "types": [
         *       {
         *         "name": "AddRequest",
         *         "type": "record"
         *       }
         *     ]
         *   }
         *
         * We must turn that into:
         *
         *   {
         *     "name": "AddRequest",
         *     "namespace": "example",
         *     "type": "record"
         *   }
         *
         * so that the schema's fullname becomes:
         *
         *   example.AddRequest
         */

        let normalized_types: Vec<JsonValue> = types
            .iter()
            .map(|type_json| {
                let mut type_json = type_json.clone();

                if let Some(object) = type_json.as_object_mut() {
                    /*
                     * Only add the protocol namespace when the type
                     * doesn't already specify one.
                     */
                    if !object.contains_key("namespace") && !namespace.is_empty() {
                        object.insert(
                            "namespace".to_string(),
                            JsonValue::String(namespace.to_string()),
                        );
                    }
                }

                type_json
            })
            .collect();

        let type_strings: Vec<String> = normalized_types
            .iter()
            .map(serde_json::to_string)
            .collect::<Result<_, _>>()?;

        /*
         * Parse all named types together.
         *
         * This allows cross-references between the protocol types.
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
             *
             * The fields generated by Avro IDL contain simple names:
             *
             *   "type": "AddRequest"
             *
             * Because this wrapper is in the "example" namespace,
             * Avro resolves that to:
             *
             *   example.AddRequest
             *
             * The namespace on the wrapper is therefore important.
             */

            let request_schema_json = serde_json::json!({
                "type": "record",
                "name": format!("RpcRequest_{}", method_name),
                "namespace": namespace,
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

            let response_name = message
                .get("response")
                .and_then(JsonValue::as_str)
                .ok_or_else(|| format!("method '{method_name}' has invalid response"))?;

            /*
             * A response such as:
             *
             *   "AddResponse"
             *
             * is relative to the protocol namespace.
             *
             * Therefore:
             *
             *   AddResponse
             *
             * becomes:
             *
             *   example.AddResponse
             */

            let response_fullname = if response_name.contains('.') || namespace.is_empty() {
                response_name.to_string()
            } else {
                format!("{namespace}.{response_name}")
            };

            /*
             * Find the parsed schema by its fullname.
             *
             * In apache-avro 0.22.0, Schema::name() returns Option<&Name>,
             * and Name::fullname() requires a default namespace argument.
             *
             * To avoid depending on that API here, compare against the
             * normalized JSON we used to construct the schemas.
             */

            let response_schema = schemata
                .iter()
                .zip(normalized_types.iter())
                .find(|(_, type_json)| {
                    let name = type_json.get("name").and_then(JsonValue::as_str);

                    let type_namespace = type_json
                        .get("namespace")
                        .and_then(JsonValue::as_str)
                        .unwrap_or("");

                    let fullname = match name {
                        Some(name) if name.contains('.') => name.to_string(),
                        Some(name) if !type_namespace.is_empty() => {
                            format!("{type_namespace}.{name}")
                        }
                        Some(name) => name.to_string(),
                        None => return false,
                    };

                    fullname == response_fullname
                })
                .map(|(schema, _)| schema)
                .cloned()
                .ok_or_else(|| format!("unknown response schema: {response_fullname}"))?;

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

    /// Request (writer) schema for `method`, cloned for caller-owned use.
    pub fn request_schema(&self, method: &str) -> RpcResult<Schema> {
        Ok(self.message(method)?.request_schema.clone())
    }

    /// Response (reader) schema for `method`, cloned for caller-owned use.
    pub fn response_schema(&self, method: &str) -> RpcResult<Schema> {
        Ok(self.message(method)?.response_schema.clone())
    }

    /*
     * ----------------------------------------------------------------------
     * Schema references
     * ----------------------------------------------------------------------
     */

    fn schemata_refs(&self) -> Vec<&Schema> {
        self.schemata.iter().collect()
    }

    /// Public accessor for writer/reader resolution helpers.
    pub fn schemata_refs_pub(&self) -> Vec<&Schema> {
        self.schemata_refs()
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

    /*
     * Server protocol as announced via handshake `serverProtocol`.
     *
     * Present after the first NONE/CLIENT handshake. Used as the
     * writer schema when decoding responses so that added fields
     * with defaults resolve correctly (reader = local protocol).
     */
    peer_protocol: Option<AvroProtocol>,

    /*
     * Last handshake `match` value (BOTH/CLIENT/NONE), for tests.
     */
    last_match: Option<String>,
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
            peer_protocol: None,
            last_match: None,
        })
    }

    /// Last handshake match (BOTH/CLIENT/NONE), if any yet.
    pub fn last_handshake_match(&self) -> Option<&str> {
        self.last_match.as_deref()
    }

    /// Whether a peer (server) protocol was announced via handshake.
    pub fn has_peer_protocol(&self) -> bool {
        self.peer_protocol.is_some()
    }

    /*
     * Remember server hash + peer protocol from a handshake response.
     */
    fn remember_handshake(&mut self, hs: &HandshakeResponse) -> RpcResult<()> {
        self.last_match = Some(hs.match_type.clone());

        if let Some(hash) = hs.server_hash {
            self.server_hash = hash;
        }

        if let Some(text) = hs.server_protocol.as_deref() {
            match AvroProtocol::parse(text) {
                Ok(peer) => {
                    self.server_hash = peer.md5;
                    self.peer_protocol = Some(peer);
                }
                Err(e) => {
                    // Keep going with hash-only behavior; response
                    // decoding falls back to the local schema.
                    eprintln!("warning: ignoring unparsable serverProtocol: {e}");
                }
            }
        }

        Ok(())
    }

    /// Writer (server) response schema for resolution. Falls back to local.
    fn writer_response_schema(&self, method: &str) -> RpcResult<Schema> {
        if let Some(peer) = self.peer_protocol.as_ref() {
            match peer.response_schema(method) {
                Ok(schema) => return Ok(schema),
                Err(_) => {
                    // Unknown in peer protocol; fall back to local so the
                    // subsequent read surfaces a clear error.
                }
            }
        }

        self.protocol.response_schema(method)
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
     */

    pub fn call(&mut self, method: &str, request: Value) -> RpcResult<Value> {
        /*
         * Get the method definition from the local .avpr.
         *
         * Requests are always encoded with the local (writer) schema;
         * the server resolves writer -> reader itself (see Python
         * Responder.read_request). Responses are decoded with
         * writer = peer protocol (if announced) and reader = local.
         */

        let request_schema = self.protocol.request_schema(method)?;

        let response_reader_schema = self.protocol.response_schema(method)?;

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
         */

        let handshake = self.encode_handshake_request(!self.handshake_complete)?;

        /*
         * --------------------------------------------------------------
         * Handshake + CallRequest
         * --------------------------------------------------------------
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
             * ==========================================================
             */
            "BOTH" => {
                self.remember_handshake(&handshake_response)?;

                self.handshake_complete = true;

                self.decode_remaining_response(&response, &cursor, method, &response_reader_schema)
            }

            /*
             * ==========================================================
             * CLIENT
             * ==========================================================
             */
            "CLIENT" => {
                self.remember_handshake(&handshake_response)?;

                self.handshake_complete = true;

                self.decode_remaining_response(&response, &cursor, method, &response_reader_schema)
            }

            /*
             * ==========================================================
             * NONE
             * ==========================================================
             */
            "NONE" => {
                self.remember_handshake(&handshake_response)?;

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
                        self.remember_handshake(&retry_handshake)?;

                        self.handshake_complete = true;

                        self.decode_remaining_response(
                            &response,
                            &cursor,
                            method,
                            &response_reader_schema,
                        )
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

        let result = GenericDatumWriter::builder(&schema)
            .build()?
            .write_value_to_vec(value)?;

        Ok(result)
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

        let value = GenericDatumReader::builder(&schema)
            .maybe_reader_schema(None)
            .build()?
            .read_value(cursor)?;

        let fields = match value {
            Value::Record(fields) => fields,

            other => {
                return Err(format!("invalid handshake response: {other:?}").into());
            }
        };

        let mut match_type = None;

        let mut server_hash = None;

        let mut server_protocol = None;

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
                    if let Value::Union(_, boxed) = value
                        && let Value::Fixed(_, bytes) = *boxed
                    {
                        let hash: [u8; 16] = bytes
                            .as_slice()
                            .try_into()
                            .map_err(|_| "serverHash must contain 16 bytes")?;

                        server_hash = Some(hash);
                    }
                }

                /*
                 * ------------------------------------------------------
                 * serverProtocol
                 * ------------------------------------------------------
                 *
                 * Union [null, string]: branch 1 carries the JSON text
                 * of the server's protocol (sent on CLIENT/NONE).
                 */
                "serverProtocol" => {
                    if let Value::Union(branch, boxed) = value
                        && branch == 1
                        && let Value::String(text) = *boxed
                    {
                        server_protocol = Some(text);
                    }
                }

                _ => {}
            }
        }

        let match_type = match_type.ok_or("handshake response missing match")?;

        Ok(HandshakeResponse {
            match_type,
            server_hash,
            server_protocol,
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
        response_reader_schema: &Schema,
    ) -> RpcResult<Value> {
        let position = usize::try_from(cursor.position())?;

        let remaining = &response[position..];

        if remaining.is_empty() {
            return Err("handshake succeeded but server returned no RPC response".into());
        }

        self.decode_call_response(remaining, method, response_reader_schema)
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

        let metadata = GenericDatumWriter::builder(&metadata_schema)
            .build()?
            .write_value_to_vec(Value::Map(HashMap::new()))?;

        /*
         * --------------------------------------------------------------
         * Method name
         * --------------------------------------------------------------
         */

        let method_bytes = GenericDatumWriter::builder(&Schema::String)
            .build()?
            .write_value_to_vec(Value::String(method.to_string()))?;

        /*
         * --------------------------------------------------------------
         * Parameters
         * --------------------------------------------------------------
         */

        let request_bytes = GenericDatumWriter::builder(request_schema)
            .schemata(self.protocol.schemata_refs())?
            .build()?
            .write_value_to_vec(request)?;

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
        response_reader_schema: &Schema,
    ) -> RpcResult<Value> {
        let mut cursor = Cursor::new(bytes);

        /*
         * --------------------------------------------------------------
         * Metadata
         * --------------------------------------------------------------
         */

        let metadata_schema = Schema::parse_str(MAP_BYTES_SCHEMA_JSON)?;

        let _metadata = GenericDatumReader::builder(&metadata_schema)
            .maybe_reader_schema(None)
            .build()?
            .read_value(&mut cursor)?;

        /*
         * --------------------------------------------------------------
         * Error flag
         * --------------------------------------------------------------
         */

        let error_value = GenericDatumReader::builder(&Schema::Boolean)
            .maybe_reader_schema(None)
            .build()?
            .read_value(&mut cursor)?;

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
            let error = GenericDatumReader::builder(&Schema::String)
                .maybe_reader_schema(None)
                .build()?
                .read_value(&mut cursor)?;

            return Err(format!("remote Avro RPC error from '{method}': {error:?}").into());
        }

        /*
         * --------------------------------------------------------------
         * Successful response with writer -> reader resolution
         * --------------------------------------------------------------
         *
         * writer = server (peer) protocol when announced via handshake,
         * reader = local protocol. This is what fills response defaults
         * (e.g. v2 `note`) when talking to a newer server, and drops
         * extra writer fields when talking to one.
         */

        let writer_schema = self.writer_response_schema(method)?;

        let writer_schemata: Vec<&Schema> = if let Some(peer) = self.peer_protocol.as_ref() {
            peer.schemata_refs_pub()
        } else {
            self.protocol.schemata_refs_pub()
        };

        let reader_schemata: Vec<&Schema> = self.protocol.schemata_refs_pub();

        let result = GenericDatumReader::builder(&writer_schema)
            .writer_schemata(writer_schemata)?
            .maybe_reader_schema(Some(response_reader_schema))
            .reader_schemata(reader_schemata)?
            .build()?
            .read_value(&mut cursor)?;

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

    server_protocol: Option<String>,
}

/*
 * ==========================================================================
 * Protocol-evolution unit tests (datum level, no network)
 * ==========================================================================
 */

#[cfg(test)]
mod evolution_tests {
    use super::*;
    use apache_avro::writer::datum::GenericDatumWriter;

    const V1: &str = include_str!("../../protocol/example_v1.avpr");
    const V2: &str = include_str!("../../protocol/example_v2.avpr");
    const V3_BAD: &str = include_str!("../../protocol/example_v3_bad.avpr");

    fn encode(schema: &Schema, refs: Vec<&Schema>, value: Value) -> Vec<u8> {
        GenericDatumWriter::builder(schema)
            .schemata(refs)
            .unwrap()
            .build()
            .unwrap()
            .write_value_to_vec(value)
            .unwrap()
    }

    fn decode(
        writer: &Schema,
        writer_refs: Vec<&Schema>,
        reader: &Schema,
        reader_refs: Vec<&Schema>,
        bytes: &[u8],
    ) -> Result<Value, String> {
        let mut cursor = Cursor::new(bytes);
        GenericDatumReader::builder(writer)
            .writer_schemata(writer_refs)
            .map_err(|e| e.to_string())?
            .maybe_reader_schema(Some(reader))
            .reader_schemata(reader_refs)
            .map_err(|e| e.to_string())?
            .build()
            .map_err(|e| e.to_string())?
            .read_value(&mut cursor)
            .map_err(|e| e.to_string())
    }

    fn inner_fields(value: &Value) -> Vec<(String, Value)> {
        match value {
            Value::Record(fields) => {
                assert_eq!(
                    fields.len(),
                    1,
                    "expected RpcRequest wrapper, got {value:?}"
                );
                match &fields[0].1 {
                    Value::Record(inner) => inner.clone(),
                    other => panic!("expected inner record, got {other:?}"),
                }
            }
            other => panic!("expected wrapper record, got {other:?}"),
        }
    }

    fn field(fields: &[(String, Value)], name: &str) -> Value {
        fields
            .iter()
            .find(|(n, _)| n == name)
            .unwrap_or_else(|| panic!("missing field '{name}' in {fields:?}"))
            .1
            .clone()
    }

    #[test]
    fn request_v1_to_v2_fills_default_c() {
        let v1 = AvroProtocol::parse(V1).unwrap();
        let v2 = AvroProtocol::parse(V2).unwrap();
        let writer = v1.request_schema("test").unwrap();
        let reader = v2.request_schema("test").unwrap();

        let datum = Value::Record(vec![(
            "request".to_string(),
            Value::Record(vec![
                ("a".to_string(), Value::Int(42)),
                ("b".to_string(), Value::Int(7)),
            ]),
        )]);
        let bytes = encode(&writer, v1.schemata_refs_pub(), datum);

        let resolved = decode(
            &writer,
            v1.schemata_refs_pub(),
            &reader,
            v2.schemata_refs_pub(),
            &bytes,
        )
        .expect("v1 -> v2 request must resolve with default c");
        let inner = inner_fields(&resolved);
        assert_eq!(field(&inner, "a"), Value::Int(42));
        assert_eq!(field(&inner, "b"), Value::Int(7));
        assert_eq!(field(&inner, "c"), Value::String("new".to_string()));
    }

    #[test]
    fn request_v2_to_v1_ignores_extra_c() {
        let v1 = AvroProtocol::parse(V1).unwrap();
        let v2 = AvroProtocol::parse(V2).unwrap();
        let writer = v2.request_schema("test").unwrap();
        let reader = v1.request_schema("test").unwrap();

        let datum = Value::Record(vec![(
            "request".to_string(),
            Value::Record(vec![
                ("a".to_string(), Value::Int(1)),
                ("b".to_string(), Value::Int(2)),
                ("c".to_string(), Value::String("hi".to_string())),
            ]),
        )]);
        let bytes = encode(&writer, v2.schemata_refs_pub(), datum);

        let resolved = decode(
            &writer,
            v2.schemata_refs_pub(),
            &reader,
            v1.schemata_refs_pub(),
            &bytes,
        )
        .expect("v2 -> v1 request must drop extra c");
        let inner = inner_fields(&resolved);
        assert_eq!(inner.len(), 2);
        assert_eq!(field(&inner, "a"), Value::Int(1));
        assert_eq!(field(&inner, "b"), Value::Int(2));
    }

    #[test]
    fn request_v1_to_v3_without_default_fails() {
        let v1 = AvroProtocol::parse(V1).unwrap();
        let v3 = AvroProtocol::parse(V3_BAD).unwrap();
        let writer = v1.request_schema("test").unwrap();
        let reader = v3.request_schema("test").unwrap();

        let datum = Value::Record(vec![(
            "request".to_string(),
            Value::Record(vec![
                ("a".to_string(), Value::Int(1)),
                ("b".to_string(), Value::Int(2)),
            ]),
        )]);
        let bytes = encode(&writer, v1.schemata_refs_pub(), datum);

        let err = decode(
            &writer,
            v1.schemata_refs_pub(),
            &reader,
            v3.schemata_refs_pub(),
            &bytes,
        )
        .expect_err("v1 -> v3 (required c, no default) must fail");
        assert!(
            err.to_lowercase().contains("default") || err.to_lowercase().contains("c"),
            "expected default/missing-field error, got: {err}"
        );
    }

    #[test]
    fn response_v1_to_v2_fills_default_note() {
        let v1 = AvroProtocol::parse(V1).unwrap();
        let v2 = AvroProtocol::parse(V2).unwrap();
        let writer = v1.response_schema("test").unwrap();
        let reader = v2.response_schema("test").unwrap();

        let bytes = encode(
            &writer,
            v1.schemata_refs_pub(),
            Value::Record(vec![("result".to_string(), Value::Int(35))]),
        );

        let resolved = decode(
            &writer,
            v1.schemata_refs_pub(),
            &reader,
            v2.schemata_refs_pub(),
            &bytes,
        )
        .expect("v1 -> v2 response must fill default note");
        match resolved {
            Value::Record(fields) => {
                assert_eq!(field(&fields, "result"), Value::Int(35));
                assert_eq!(field(&fields, "note"), Value::String("n/a".to_string()));
            }
            other => panic!("expected record, got {other:?}"),
        }
    }

    #[test]
    fn response_v2_to_v1_ignores_extra_note() {
        let v1 = AvroProtocol::parse(V1).unwrap();
        let v2 = AvroProtocol::parse(V2).unwrap();
        let writer = v2.response_schema("test").unwrap();
        let reader = v1.response_schema("test").unwrap();

        let bytes = encode(
            &writer,
            v2.schemata_refs_pub(),
            Value::Record(vec![
                ("result".to_string(), Value::Int(35)),
                ("note".to_string(), Value::String("c=new".to_string())),
            ]),
        );

        let resolved = decode(
            &writer,
            v2.schemata_refs_pub(),
            &reader,
            v1.schemata_refs_pub(),
            &bytes,
        )
        .expect("v2 -> v1 response must drop extra note");
        match resolved {
            Value::Record(fields) => {
                assert_eq!(fields.len(), 1);
                assert_eq!(field(&fields, "result"), Value::Int(35));
            }
            other => panic!("expected record, got {other:?}"),
        }
    }
}
