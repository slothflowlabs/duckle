//! gRPC: one call to a method a server reflects, so neither a .proto file nor
//! protoc is needed. The server's reflection service hands over the files that
//! describe the method's messages, the request is built from JSON against them,
//! and each response becomes a row.

use crate::*;
use prost::Message as _;
use prost_reflect::{DescriptorPool, DynamicMessage, MessageDescriptor, SerializeOptions};
use tonic_reflection::pb::v1::{
    server_reflection_client::ServerReflectionClient, server_reflection_request::MessageRequest,
    server_reflection_response::MessageResponse, ServerReflectionRequest,
};

/// A codec for messages known only at run time: encoded as they are, decoded
/// against the method's output.
struct DynamicCodec(MessageDescriptor);
struct DynamicEncoder;
struct DynamicDecoder(MessageDescriptor);

impl tonic::codec::Encoder for DynamicEncoder {
    type Item = DynamicMessage;
    type Error = tonic::Status;
    fn encode(&mut self, item: DynamicMessage, dst: &mut tonic::codec::EncodeBuf<'_>) -> Result<(), tonic::Status> {
        item.encode(dst).map_err(|e| tonic::Status::internal(e.to_string()))
    }
}

impl tonic::codec::Decoder for DynamicDecoder {
    type Item = DynamicMessage;
    type Error = tonic::Status;
    fn decode(&mut self, src: &mut tonic::codec::DecodeBuf<'_>) -> Result<Option<DynamicMessage>, tonic::Status> {
        DynamicMessage::decode(self.0.clone(), src).map(Some).map_err(|e| tonic::Status::internal(e.to_string()))
    }
}

impl tonic::codec::Codec for DynamicCodec {
    type Encode = DynamicMessage;
    type Decode = DynamicMessage;
    type Encoder = DynamicEncoder;
    type Decoder = DynamicDecoder;
    fn encoder(&mut self) -> DynamicEncoder {
        DynamicEncoder
    }
    fn decoder(&mut self) -> DynamicDecoder {
        DynamicDecoder(self.0.clone())
    }
}

/// `f`, given up on after `ms` milliseconds without an answer, so a server that
/// takes the connection and never answers fails the run rather than holding it.
async fn within<T>(ms: u64, what: &str, f: impl std::future::Future<Output = Result<T, String>>) -> Result<T, String> {
    tokio::time::timeout(std::time::Duration::from_millis(ms), f)
        .await
        .map_err(|_| format!("{what}: no answer within {ms} ms"))?
}

/// A status as gRPC names its code, with what the server said.
fn status(s: tonic::Status) -> String {
    format!("{:?}: {}", s.code(), s.message())
}

/// The node's metadata on a request.
fn with_metadata<T>(mut request: tonic::Request<T>, metadata: &[(String, String)]) -> Result<tonic::Request<T>, String> {
    for (key, value) in metadata {
        let key = tonic::metadata::MetadataKey::from_bytes(key.as_bytes())
            .map_err(|_| format!("metadata key {key:?} is not one gRPC takes"))?;
        let value = tonic::metadata::MetadataValue::try_from(value.as_str())
            .map_err(|_| format!("the value of metadata {key} is not one gRPC takes"))?;
        request.metadata_mut().insert(key, value);
    }
    Ok(request)
}

/// The descriptors of `service` and of every file it imports, asked of the
/// server's reflection: the file that holds the service, then each import the
/// server has not sent yet, by name.
async fn reflect(
    channel: tonic::transport::Channel,
    service: &str,
    metadata: &[(String, String)],
) -> Result<DescriptorPool, String> {
    let mut client = ServerReflectionClient::new(channel);
    let mut files: std::collections::HashMap<String, prost_reflect::prost_types::FileDescriptorProto> = Default::default();
    let mut wanted = vec![MessageRequest::FileContainingSymbol(service.to_string())];
    while !wanted.is_empty() {
        let asks: Vec<ServerReflectionRequest> = wanted
            .drain(..)
            .map(|m| ServerReflectionRequest { host: String::new(), message_request: Some(m) })
            .collect();
        let request = with_metadata(tonic::Request::new(futures_util::stream::iter(asks)), metadata)?;
        let mut answers = client
            .server_reflection_info(request)
            .await
            .map_err(|s| format!("the server's reflection: {}", status(s)))?
            .into_inner();
        while let Some(answer) = answers.message().await.map_err(|s| format!("the server's reflection: {}", status(s)))? {
            match answer.message_response {
                Some(MessageResponse::FileDescriptorResponse(found)) => {
                    for bytes in found.file_descriptor_proto {
                        let file = prost_reflect::prost_types::FileDescriptorProto::decode(bytes.as_slice())
                            .map_err(|e| format!("a descriptor the server reflected: {e}"))?;
                        files.insert(file.name().to_string(), file);
                    }
                }
                Some(MessageResponse::ErrorResponse(e)) => {
                    return Err(format!("the server's reflection does not know {service}: {}", e.error_message))
                }
                _ => {}
            }
        }
        let mut missing: Vec<String> =
            files.values().flat_map(|f| f.dependency.iter()).filter(|d| !files.contains_key(*d)).cloned().collect();
        missing.sort();
        missing.dedup();
        wanted = missing.into_iter().map(MessageRequest::FileByFilename).collect();
    }
    let mut pool = DescriptorPool::new();
    pool.add_file_descriptor_protos(files.into_values()).map_err(|e| format!("the descriptors the server reflected: {e}"))?;
    Ok(pool)
}

impl DuckdbEngine {
    pub(crate) fn run_grpc_source(&self, db: &Path, spec: &plan::GrpcSourceSpec) -> Result<String, EngineError> {
        self.check_cancelled()?;
        crate::policy::outbound_host_allowed(&spec.endpoint).map_err(|e| EngineError::Query(format!("grpc: {e}")))?;
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| EngineError::Query(format!("grpc: tokio rt: {e}")))?;
        let called: Result<(Vec<JsonValue>, Vec<String>), String> = rt.block_on(async {
            let mut endpoint = tonic::transport::Endpoint::from_shared(spec.endpoint.clone())
                .map_err(|e| format!("{}: {e}", spec.endpoint))?
                .connect_timeout(std::time::Duration::from_secs(10));
            if spec.endpoint.starts_with("https://") {
                endpoint = endpoint
                    .tls_config(tonic::transport::ClientTlsConfig::new().with_native_roots())
                    .map_err(|e| format!("{}: {e}", spec.endpoint))?;
            }
            let channel = within(spec.timeout_ms, &spec.endpoint, async {
                endpoint.connect().await.map_err(|e| format!("{}: {e}", spec.endpoint))
            })
            .await?;
            let pool =
                within(spec.timeout_ms, "the server's reflection", reflect(channel.clone(), &spec.service, &spec.metadata))
                    .await?;
            let service = pool
                .get_service_by_name(&spec.service)
                .ok_or_else(|| format!("the server reflects no service {}", spec.service))?;
            let Some(method) = service.methods().find(|m| m.name() == spec.method) else {
                let has: Vec<String> = service.methods().map(|m| m.name().to_string()).collect();
                return Err(format!("{} has no method {}; it has {}", spec.service, spec.method, has.join(", ")));
            };
            if method.is_client_streaming() {
                return Err(format!("{} takes a stream of requests, which src.grpc does not send", method.full_name()));
            }
            let message = DynamicMessage::deserialize(method.input(), spec.request.clone())
                .map_err(|e| format!("the request is not a {}: {e}", method.input().full_name()))?;
            let path = tonic::codegen::http::uri::PathAndQuery::try_from(format!("/{}/{}", service.full_name(), method.name()))
                .map_err(|e| e.to_string())?;
            let request = with_metadata(tonic::Request::new(message), &spec.metadata)?;
            let codec = DynamicCodec(method.output());
            let mut grpc = tonic::client::Grpc::new(channel);
            within(spec.timeout_ms, &spec.endpoint, async { grpc.ready().await.map_err(|e| format!("{}: {e}", spec.endpoint)) })
                .await?;
            let call = format!("{}/{}", spec.service, spec.method);
            let mut responses = Vec::new();
            if method.is_server_streaming() {
                let mut stream =
                    within(spec.timeout_ms, &call, async move { grpc.server_streaming(request, path, codec).await.map_err(status) })
                        .await?
                        .into_inner();
                while (responses.len() as u64) < spec.max_messages {
                    match tokio::time::timeout(std::time::Duration::from_millis(spec.timeout_ms), stream.message()).await {
                        Err(_) | Ok(Ok(None)) => break, // quiet for the timeout, or the stream ended
                        Ok(Ok(Some(m))) => responses.push(m),
                        Ok(Err(s)) => return Err(status(s)),
                    }
                }
            } else {
                responses.push(
                    within(spec.timeout_ms, &call, async move { grpc.unary(request, path, codec).await.map_err(status) })
                        .await?
                        .into_inner(),
                );
            }
            // Every field, by its proto name, and enum values by name.
            let options = SerializeOptions::new().skip_default_fields(false).use_proto_field_name(true);
            let rows = responses
                .iter()
                .map(|m| m.serialize_with_options(serde_json::value::Serializer, &options).map_err(|e| e.to_string()))
                .collect::<Result<Vec<JsonValue>, String>>()?;
            let columns = method.output().fields().map(|f| f.name().to_string()).collect();
            Ok((rows, columns))
        });
        let (rows, columns) = called.map_err(|e| EngineError::Query(format!("grpc: {e}")))?;
        // A stream that sent nothing still reads as a table, of the response's fields.
        let empty: Vec<duckle_metadata::Column> = columns
            .into_iter()
            .map(|name| duckle_metadata::Column {
                tags: Vec::new(),
                name,
                data_type: duckle_metadata::DataType::String,
                nullable: true,
                primary_key: None,
                format: None,
            })
            .collect();
        materialize_jsonobjects_as_table_typed(&self.bin, db, &spec.node_id, &rows, Some(&empty))?;
        Ok(format!("grpc: {} response(s) from {}/{} into {}", rows.len(), spec.service, spec.method, spec.node_id))
    }
}
