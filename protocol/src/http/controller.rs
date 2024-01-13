use hyper::server::conn::http1;
use hyper::{body::Incoming, Request as HyperRequest, Response as HyperResponse};
use hyper_util::rt::TokioIo;
use std::net::SocketAddr;
use tokio::net::TcpListener;

use super::{
    router_tree::{RouterTree, Routers},
    routers,
};

pub type Request = HyperRequest<Incoming>;

pub type Response = HyperResponse<http_body_util::Full<bytes::Bytes>>;

pub type HandlerResult = Result<Response, hyper::Error>;

pub type Handler = fn(Request) -> HandlerResult;

#[derive(Clone)]
pub(crate) struct HttpProtocolInner {
    routers: Routers<Handler>,
}

impl HttpProtocolInner {
    pub fn new(routes: Vec<RouterTree<Handler>>) -> Self {
        let routers = Routers::from(routes);

        Self { routers }
    }

    pub async fn listen(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let addr: SocketAddr = ([127, 0, 0, 1], 3000).into();

        let listener = TcpListener::bind(addr).await?;

        loop {
            let (stream, _) = listener.accept().await?;
            let io: TokioIo<tokio::net::TcpStream> = TokioIo::new(stream);
            let service_clone = self.clone();

            tokio::task::spawn(async move {
                if let Err(err) = http1::Builder::new()
                    .serve_connection(io, service_clone)
                    .await
                {
                    println!("Failed to serve connection: {:?}", err);
                }
            });
        }
    }

    fn resolve_routers(&self, req: Request) -> HandlerResult {
        let path = req.uri().path();
        let method = req.method();

        match self.routers.get_target(path, method) {
            Some(handler) => handler(req),
            None => routers::not_found(),
        }
    }
}

use hyper::service::Service;
use std::future::Future;
use std::pin::Pin;

impl Service<Request> for HttpProtocolInner {
    type Response = Response;
    type Error = hyper::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn call(&self, req: Request) -> Self::Future {
        let res = self.resolve_routers(req);
        Box::pin(async { res })
    }
}
