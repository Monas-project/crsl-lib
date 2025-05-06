use crate::dasl::node::Node;
use cid::Cid;
use std::collections::HashMap;
// todo: error handling
pub trait NodeStorage<P, M> {
    fn get(&self, content_id: &Cid) -> Option<Node<P, M>>;
    fn put(&mut self, node: &Node<P, M>);
    fn delete(&mut self, content_id: &Cid);
    fn get_node_map(&self) -> HashMap<Cid, Vec<Cid>>;
}
