use std::collections::HashMap;

use hyper::Method;
use regex::Regex;
#[derive(Debug, Clone)]
pub struct RouterTree<F>
where
    F: Clone + 'static,
{
    pub path: String,
    pub methods: Vec<Method>,
    pub target: F,
}

impl<F> RouterTree<F>
where
    F: Clone + 'static,
{
    pub fn new(path: &str, methods: Vec<Method>, target: F) -> Self {
        RouterTree {
            path: path.to_string(),
            methods,
            target,
        }
    }
}

#[derive(Debug, Clone)]
pub struct Routers<F> {
    tree: HashMap<Method, HashMap<String, i32>>,
    targets: HashMap<i32, F>,
}

impl<F> From<Vec<RouterTree<F>>> for Routers<F>
where
    F: Clone + 'static,
{
    fn from(mut trees: Vec<RouterTree<F>>) -> Self {
        let mut tree: HashMap<Method, HashMap<String, i32>> = HashMap::new();
        let mut targets: HashMap<i32, F> = HashMap::new();

        let mut index = 0;
        for route in trees.iter_mut() {
            for method in route.methods.iter() {
                match tree.get_mut(method) {
                    Some(tree_method) => {
                        tree_method.insert(route.path.clone(), index);
                    }
                    None => {
                        let mut tree_method = HashMap::new();
                        tree_method.insert(route.path.clone(), index);
                        tree.insert(method.clone(), tree_method);
                    }
                }
            }

            targets.insert(index, route.target.clone());
            index += 1;
        }

        Routers { tree, targets }
    }
}

impl<F> Routers<F>
where
    F: Clone + 'static,
{
    pub fn get_target(&self, path: &str, method: &Method) -> Option<F> {
        match self.tree.get(method) {
            Some(tree_method) => {
                for (route_path, index) in tree_method.iter() {
                    let re = Regex::new(route_path).unwrap();
                    if re.is_match(path) {
                        return Some(self.targets.get(index).unwrap().clone());
                    }
                }
                None
            }
            None => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn target_demo() -> String {
        "hello world".to_string()
    }

    #[test]
    fn test_router() {
        let routes = vec![
            RouterTree::new("/", vec![Method::GET], target_demo),
            RouterTree::new("/", vec![Method::DELETE], target_demo),
            RouterTree::new("/[0-9]", vec![Method::PATCH], target_demo),
            RouterTree::new("/*", vec![Method::POST, Method::PUT], target_demo),
            RouterTree::new("/posts", vec![Method::GET], target_demo),
            RouterTree::new("/posts/[a-z]", vec![Method::GET], target_demo),
            RouterTree::new("/posts/[0-9]", vec![Method::GET], target_demo),
            RouterTree::new("/posts/[0-9]", vec![Method::GET], target_demo),
        ];

        let routers = Routers::from(routes);

        assert!(routers.get_target("/", &Method::GET).is_some());
        assert!(routers.get_target("/", &Method::DELETE).is_some());
        assert!(routers.get_target("/123", &Method::PATCH).is_some());
        assert!(routers.get_target("/123", &Method::POST).is_some());
        assert!(routers.get_target("/123", &Method::PUT).is_some());
        assert!(routers.get_target("/posts", &Method::GET).is_some());
        assert!(routers.get_target("/posts/abc", &Method::GET).is_some());
        assert!(routers.get_target("/posts/123", &Method::GET).is_some());
        assert!(routers.get_target("/posts/123", &Method::GET).is_some());
    }
}