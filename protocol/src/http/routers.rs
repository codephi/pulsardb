use bytes::Bytes;
use http_body_util::Full;
use hyper::{Response, Request, body::Incoming};

use super::responses::ErrorMessage;

pub fn not_found() -> Result<Response<Full<Bytes>>, hyper::Error> {
    Ok(ErrorMessage::not_found().into())
}

pub fn mk_response(r: Request<Incoming>) -> Result<Response<Full<Bytes>>, hyper::Error> {
    Ok(Response::builder().body(Full::new(Bytes::from("example"))).unwrap())
}


