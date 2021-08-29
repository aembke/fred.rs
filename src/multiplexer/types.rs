use std::sync::Arc;

/// Options describing how to change connections in a cluster.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClusterChange {
  pub add: Vec<Arc<String>>,
  pub remove: Vec<Arc<String>>,
}

impl Default for ClusterChange {
  fn default() -> Self {
    ClusterChange {
      add: Vec::new(),
      remove: Vec::new(),
    }
  }
}
