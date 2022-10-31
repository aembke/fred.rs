use arcstr::ArcStr;

/// Options describing how to change connections in a cluster.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClusterChange {
  pub add:    Vec<ArcStr>,
  pub remove: Vec<ArcStr>,
}

impl Default for ClusterChange {
  fn default() -> Self {
    ClusterChange {
      add:    Vec::new(),
      remove: Vec::new(),
    }
  }
}
