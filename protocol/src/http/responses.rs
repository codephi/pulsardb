use bytes::Bytes;
use http_body_util::Full;
use hyper::{StatusCode, Response};
use valu3::prelude::*;

#[derive(ToValue, ToJson, Debug, Clone)]
pub struct ErrorMessage {
    message: String,
    status_code: u16,
}

impl Into<Bytes> for ErrorMessage {
    fn into(self) -> Bytes {
        Bytes::from(self.to_json())
    }
}

impl Into<Response<Full<Bytes>>> for ErrorMessage {
    fn into(self) -> Response<Full<Bytes>> {
        Response::builder()
            .status(StatusCode::from_u16(self.status_code).unwrap())
            .body(Full::new(self.into()))
            .unwrap()
    }
}

impl ErrorMessage {
    fn new(message: String, status_code: StatusCode) -> Self {
        ErrorMessage {
            message,
            status_code: status_code.as_u16(),
        }
    }

    pub fn not_found() -> Self {
        ErrorMessage::new("Not Found".to_string(), StatusCode::NOT_FOUND)
    }

    pub fn bad_request() -> Self {
        ErrorMessage::new("Bad Request".to_string(), StatusCode::BAD_REQUEST)
    }

    pub fn unauthorized() -> Self {
        ErrorMessage::new("Unauthorized".to_string(), StatusCode::UNAUTHORIZED)
    }

    pub fn forbidden() -> Self {
        ErrorMessage::new("Forbidden".to_string(), StatusCode::FORBIDDEN)
    }

    pub fn method_not_allowed() -> Self {
        ErrorMessage::new(
            "Method Not Allowed".to_string(),
            StatusCode::METHOD_NOT_ALLOWED,
        )
    }

    pub fn not_acceptable() -> Self {
        ErrorMessage::new("Not Acceptable".to_string(), StatusCode::NOT_ACCEPTABLE)
    }

    pub fn conflict() -> Self {
        ErrorMessage::new("Conflict".to_string(), StatusCode::CONFLICT)
    }

    pub fn unsupported_media_type() -> Self {
        ErrorMessage::new(
            "Unsupported Media Type".to_string(),
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
        )
    }

    pub fn unprocessable_entity() -> Self {
        ErrorMessage::new(
            "Unprocessable Entity".to_string(),
            StatusCode::UNPROCESSABLE_ENTITY,
        )
    }

    pub fn too_many_requests() -> Self {
        ErrorMessage::new(
            "Too Many Requests".to_string(),
            StatusCode::TOO_MANY_REQUESTS,
        )
    }

    pub fn internal_server_error() -> Self {
        ErrorMessage::new(
            "Internal Server Error".to_string(),
            StatusCode::INTERNAL_SERVER_ERROR,
        )
    }

    pub fn service_unavailable() -> Self {
        ErrorMessage::new(
            "Service Unavailable".to_string(),
            StatusCode::SERVICE_UNAVAILABLE,
        )
    }

    pub fn gateway_timeout() -> Self {
        ErrorMessage::new("Gateway Timeout".to_string(), StatusCode::GATEWAY_TIMEOUT)
    }
}
