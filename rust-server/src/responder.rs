use apache_avro::Schema;
use apache_avro::reader::datum::GenericDatumReader;
use apache_avro::types::Value;
use apache_avro::writer::datum::GenericDatumWriter;
use rust_avro_rpc::avro_rpc::{
    AvroProtocol, HANDSHAKE_REQUEST_SCHEMA_JSON, HANDSHAKE_RESPONSE_SCHEMA_JSON,
    MAP_BYTES_SCHEMA_JSON,
};
use std::collections::HashMap;
use std::io::Cursor;
use std::sync::{Arc, Mutex};

type ServerResult<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

const PROTOCOL: &str = include_str!("../../protocol/example.avpr");

/*
 * ==========================================================================
 * Example Avro RPC responder (server side)
 * ==========================================================================
 *
 * Mirrors the Python `avro.ipc.Responder` used by this project:
 *
 *   handshake -> schema resolution (writer = remote, reader = local)
 *   -> dispatch -> strict response encoding.
 */

pub struct ExampleResponder {
    local: Arc<AvroProtocol>,

    /*
     * Remote (client) protocols seen so far, keyed by clientHash.
     */
    cache: Mutex<HashMap<[u8; 16], AvroProtocol>>,

    handshake_request_schema: Schema,
    handshake_response_schema: Schema,
    metadata_schema: Schema,
}

impl ExampleResponder {
    pub fn new() -> ServerResult<Self> {
        Self::with_protocol(PROTOCOL)
    }

    pub fn with_protocol(protocol_text: &str) -> ServerResult<Self> {
        let local = Arc::new(AvroProtocol::parse(protocol_text)?);

        Ok(Self {
            local,
            cache: Mutex::new(HashMap::new()),
            handshake_request_schema: Schema::parse_str(HANDSHAKE_REQUEST_SCHEMA_JSON)?,
            handshake_response_schema: Schema::parse_str(HANDSHAKE_RESPONSE_SCHEMA_JSON)?,
            metadata_schema: Schema::parse_str(MAP_BYTES_SCHEMA_JSON)?,
        })
    }

    /*
     * ----------------------------------------------------------------------
     * Process one framed CallRequest payload, return the framed CallResponse
     * payload (handshake + metadata + error flag + body).
     * ----------------------------------------------------------------------
     */

    pub fn respond(&self, request: &[u8]) -> Vec<u8> {
        match self.respond_inner(request) {
            Ok(bytes) => bytes,
            Err(error) => self.encode_system_error(&error.to_string()),
        }
    }

    fn respond_inner(&self, request: &[u8]) -> ServerResult<Vec<u8>> {
        let mut cursor = Cursor::new(request);

        /*
         * --------------------------------------------------------------
         * Handshake
         * --------------------------------------------------------------
         */

        let (client_hash, client_protocol, server_hash_guess) =
            self.decode_handshake_request(&mut cursor)?;

        /*
         * Remember remote protocols so later requests on new connections
         * without an embedded protocol still resolve.
         */

        let remote = self.remote_protocol(&client_hash, client_protocol.as_deref())?;

        let match_type = if server_hash_guess == self.local.md5 {
            if remote.is_some() { "BOTH" } else { "NONE" }
        } else if remote.is_some() {
            "CLIENT"
        } else {
            "NONE"
        };

        let mut response = self.encode_handshake_response(match_type)?;

        if match_type == "NONE" {
            return Ok(response);
        }

        /*
         * --------------------------------------------------------------
         * CallRequest: metadata + method + params
         * --------------------------------------------------------------
         *
         * From here on, failures become remote errors (error flag + string)
         * instead of transport-level failures, so the client gets a readable
         * message and the connection stays alive.
         */

        let call_response = match self.decode_and_invoke(&mut cursor, remote) {
            Ok(payload) => self.encode_success(&payload)?,
            Err(error) => self.encode_error_body(&error.to_string()),
        };

        response.extend_from_slice(&call_response);

        Ok(response)
    }

    /*
     * ----------------------------------------------------------------------
     * Remote protocol lookup
     * ----------------------------------------------------------------------
     */

    fn remote_protocol(
        &self,
        client_hash: &[u8; 16],
        client_protocol: Option<&str>,
    ) -> ServerResult<Option<AvroProtocol>> {
        let mut cache = self
            .cache
            .lock()
            .map_err(|_| "protocol cache lock poisoned")?;

        if let Some(protocol) = cache.get(client_hash) {
            return Ok(Some(protocol.clone()));
        }

        if let Some(text) = client_protocol {
            let protocol = AvroProtocol::parse(text)?;
            cache.insert(*client_hash, protocol.clone());
            return Ok(Some(protocol));
        }

        Ok(None)
    }

    /*
     * ----------------------------------------------------------------------
     * Decode + dispatch + encode body
     * ----------------------------------------------------------------------
     */

    fn decode_and_invoke(
        &self,
        cursor: &mut Cursor<&[u8]>,
        remote: Option<AvroProtocol>,
    ) -> ServerResult<Vec<u8>> {
        let remote = remote.ok_or("unknown remote protocol")?;

        /*
         * Metadata (ignored, but must be consumed).
         */

        GenericDatumReader::builder(&self.metadata_schema)
            .maybe_reader_schema(None)
            .build()?
            .read_value(cursor)?;

        /*
         * Method name.
         */

        let method_value = GenericDatumReader::builder(&Schema::String)
            .maybe_reader_schema(None)
            .build()?
            .read_value(cursor)?;

        let method = match method_value {
            Value::String(method) => method,
            other => return Err(format!("invalid Avro RPC method name: {other:?}").into()),
        };

        /*
         * Parameters with writer -> reader resolution.
         *
         * writer = remote (client) request schema,
         * reader = local (server) request schema.
         *
         * Added fields with defaults (e.g. v2 `c`) are filled here;
         * missing defaults or unknown methods become remote errors.
         */

        let writer_schema = remote
            .request_schema(&method)
            .map_err(|e| format!("remote Avro RPC error: {e}"))?;

        let reader_schema = self
            .local
            .request_schema(&method)
            .map_err(|e| format!("remote Avro RPC error: {e}"))?;

        let writer_refs = remote.schemata_refs_pub();
        let reader_refs = self.local.schemata_refs_pub();

        let params = GenericDatumReader::builder(&writer_schema)
            .writer_schemata(writer_refs)?
            .maybe_reader_schema(Some(&reader_schema))
            .reader_schemata(reader_refs)?
            .build()?
            .read_value(cursor)
            .map_err(|e| format!("remote Avro RPC error: {e}"))?;

        /*
         * Dispatch.
         */

        let result = self.invoke(&method, &params)?;

        /*
         * Encode the result with exactly the local response schema fields.
         *
         * Writers are strict: the datum must match the schema, no extra or
         * missing fields.
         */

        let response_schema = self.local.response_schema(&method)?;

        GenericDatumWriter::builder(&response_schema)
            .schemata(self.local.schemata_refs_pub())?
            .build()?
            .write_value_to_vec(result)
            .map_err(Into::into)
    }

    /*
     * ----------------------------------------------------------------------
     * Method dispatch
     * ----------------------------------------------------------------------
     */

    fn invoke(&self, method: &str, params: &Value) -> ServerResult<Value> {
        match method {
            "echo" => Self::echo(params),
            "add" => Self::add(params),
            "test" => Self::test(params),
            _ => Err(format!("unknown RPC method: {method}").into()),
        }
    }

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

    /*
     * ======================================================================
     * Handshake codec
     * ======================================================================
     */

    fn decode_handshake_request(
        &self,
        cursor: &mut Cursor<&[u8]>,
    ) -> ServerResult<([u8; 16], Option<String>, [u8; 16])> {
        let value = GenericDatumReader::builder(&self.handshake_request_schema)
            .maybe_reader_schema(None)
            .build()?
            .read_value(cursor)?;

        let fields = match value {
            Value::Record(fields) => fields,
            other => return Err(format!("invalid handshake request: {other:?}").into()),
        };

        let mut client_hash = None;
        let mut client_protocol = None;
        let mut server_hash = None;

        for (name, value) in fields {
            match name.as_str() {
                "clientHash" => {
                    if let Value::Fixed(_, bytes) = value {
                        let hash: [u8; 16] = bytes
                            .as_slice()
                            .try_into()
                            .map_err(|_| "clientHash must contain 16 bytes")?;
                        client_hash = Some(hash);
                    }
                }
                "clientProtocol" => {
                    if let Value::Union(branch, boxed) = value
                        && branch == 1
                        && let Value::String(text) = *boxed
                    {
                        client_protocol = Some(text);
                    }
                }
                "serverHash" => {
                    if let Value::Fixed(_, bytes) = value {
                        let hash: [u8; 16] = bytes
                            .as_slice()
                            .try_into()
                            .map_err(|_| "serverHash must contain 16 bytes")?;
                        server_hash = Some(hash);
                    }
                }
                _ => {}
            }
        }

        let client_hash = client_hash.ok_or("handshake request missing clientHash")?;
        let server_hash = server_hash.ok_or("handshake request missing serverHash")?;

        Ok((client_hash, client_protocol, server_hash))
    }

    fn encode_handshake_response(&self, match_type: &str) -> ServerResult<Vec<u8>> {
        /*
         * HandshakeMatch symbols: BOTH = 0, CLIENT = 1, NONE = 2.
         */
        let index = match match_type {
            "BOTH" => 0,
            "CLIENT" => 1,
            "NONE" => 2,
            other => return Err(format!("unknown handshake match: {other}").into()),
        };

        let is_both = match_type == "BOTH";

        let server_protocol = if is_both {
            Value::Union(0, Box::new(Value::Null))
        } else {
            Value::Union(1, Box::new(Value::String(self.local.raw.clone())))
        };

        let server_hash = if is_both {
            Value::Union(0, Box::new(Value::Null))
        } else {
            Value::Union(1, Box::new(Value::Fixed(16, self.local.md5.to_vec())))
        };

        let value = Value::Record(vec![
            (
                "match".to_string(),
                Value::Enum(index, match_type.to_string()),
            ),
            ("serverProtocol".to_string(), server_protocol),
            ("serverHash".to_string(), server_hash),
            ("meta".to_string(), Value::Union(0, Box::new(Value::Null))),
        ]);

        GenericDatumWriter::builder(&self.handshake_response_schema)
            .build()?
            .write_value_to_vec(value)
            .map_err(Into::into)
    }

    /*
     * ======================================================================
     * CallResponse body codec
     * ======================================================================
     */

    fn encode_success(&self, payload: &[u8]) -> ServerResult<Vec<u8>> {
        let metadata = GenericDatumWriter::builder(&self.metadata_schema)
            .build()?
            .write_value_to_vec(Value::Map(HashMap::new()))?;

        let flag = GenericDatumWriter::builder(&Schema::Boolean)
            .build()?
            .write_value_to_vec(Value::Boolean(false))?;

        let mut result = Vec::with_capacity(metadata.len() + flag.len() + payload.len());
        result.extend_from_slice(&metadata);
        result.extend_from_slice(&flag);
        result.extend_from_slice(payload);

        Ok(result)
    }

    fn encode_error_body(&self, message: &str) -> Vec<u8> {
        let mut result = Vec::new();

        /*
         * Best effort: if even this encoding fails there is nothing
         * meaningful left to send, so fall back to an empty body.
         */

        let metadata: ServerResult<Vec<u8>> = GenericDatumWriter::builder(&self.metadata_schema)
            .build()
            .map_err(Into::into)
            .and_then(|writer| {
                writer
                    .write_value_to_vec(Value::Map(HashMap::new()))
                    .map_err(Into::into)
            });

        let flag: ServerResult<Vec<u8>> = GenericDatumWriter::builder(&Schema::Boolean)
            .build()
            .map_err(Into::into)
            .and_then(|writer| {
                writer
                    .write_value_to_vec(Value::Boolean(true))
                    .map_err(Into::into)
            });

        let error: ServerResult<Vec<u8>> = GenericDatumWriter::builder(&Schema::String)
            .build()
            .map_err(Into::into)
            .and_then(|writer| {
                writer
                    .write_value_to_vec(Value::String(message.to_string()))
                    .map_err(Into::into)
            });

        if let (Ok(metadata), Ok(flag), Ok(error)) = (metadata, flag, error) {
            result.extend_from_slice(&metadata);
            result.extend_from_slice(&flag);
            result.extend_from_slice(&error);
        }

        result
    }

    /*
     * Last-resort error when even the handshake cannot be decoded.
     *
     * There is no valid handshake prefix in this case, so the payload is
     * metadata + error flag + string only. The client surfaces this as a
     * decode error rather than a remote error.
     */

    fn encode_system_error(&self, message: &str) -> Vec<u8> {
        self.encode_error_body(message)
    }
}
