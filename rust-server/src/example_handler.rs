use apache_avro::types::Value;
use super::responder::{AvroHandler, AvroResponder, ServerResult};

/*
 * ==========================================================================
 * Example protocol handler (the only per-protocol code)
 * ==========================================================================
 *
 * To serve a different `.avpr`, add a new `struct MyHandler` implementing
 * `AvroHandler` and use `AvroResponder::from_protocol(text, MyHandler)`.
 */

const PROTOCOL: &str = include_str!("../../protocol/example.avpr");

pub struct ExampleHandler;

impl AvroHandler for ExampleHandler {
    fn invoke(&self, method: &str, params: &Value) -> ServerResult<Value> {
        match method {
            "echo" => ExampleHandler::echo(params),
            "add" => ExampleHandler::add(params),
            "test" => ExampleHandler::test(params),
            _ => Err(format!("unknown RPC method: {method}").into()),
        }
    }
}

pub type ExampleResponder = AvroResponder<ExampleHandler>;

impl ExampleResponder {
    pub fn new() -> ServerResult<Self> {
        Self::with_protocol(PROTOCOL)
    }

    pub fn with_protocol(protocol_text: &str) -> ServerResult<Self> {
        Self::from_protocol(protocol_text, ExampleHandler)
    }
}

impl ExampleHandler {
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
}
