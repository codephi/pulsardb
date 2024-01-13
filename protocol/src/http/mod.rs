mod controller;
mod responses;
mod router_tree;
mod routers;

use crate::http::router_tree::RouterTree;

use self::controller::Handler;

pub struct Http {
    pub router_tree: Vec<RouterTree<Handler>>,
}

impl Http {
    pub fn new() -> Self {
        Self {
            router_tree: Vec::new(),
        }
    }

    pub fn router(&mut self, router: RouterTree<Handler>) {
        self.router_tree.push(router);
    }

    pub async fn listen(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let inner = controller::HttpProtocolInner::new(self.router_tree.clone());

        inner.listen().await
    }
}