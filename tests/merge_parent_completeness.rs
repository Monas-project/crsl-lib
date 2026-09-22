use cid::Cid;
use crsl_lib::{
    convergence::{
        metadata::ContentMetadata,
        policy::{MergePolicy, ResolveInput},
    },
    crdt::{
        crdt_state::CrdtState,
        operation::{Operation, OperationType},
        storage::LeveldbStorage,
    },
    graph::{dag::DagGraph, storage::LeveldbNodeStorage},
    repo::Repo,
    storage::SharedLeveldb,
};
use std::sync::{Arc, Mutex};

type TestRepo =
    Repo<LeveldbStorage<Cid, String>, LeveldbNodeStorage<String, ContentMetadata>, String>;
type Seen = Arc<Mutex<Vec<(String, Vec<String>)>>>;
// Same parent-aware rule as the PR's installed_merge_policy_is_used_and_sees_parents.
struct Changed(Seen);
impl MergePolicy<String> for Changed {
    fn resolve(&self, nodes: &[ResolveInput<String>]) -> String {
        *self.0.lock().unwrap() = nodes
            .iter()
            .map(|n| (n.payload.clone(), n.parent_payloads.clone()))
            .collect();
        let mut changed: Vec<_> = nodes
            .iter()
            .filter(|n| n.parent_payloads.iter().all(|p| p != &n.payload))
            .map(|n| n.payload.clone())
            .collect();
        changed.sort();
        changed.join("+")
    }
    fn name(&self) -> &str {
        "changed"
    }
}
fn repo() -> (TestRepo, tempfile::TempDir, Seen) {
    let dir = tempfile::tempdir().unwrap();
    let db = SharedLeveldb::open(dir.path().join("db")).unwrap();
    let seen = Arc::new(Mutex::new(Vec::new()));
    let repo = Repo::new(
        CrdtState::new(LeveldbStorage::new(db.clone())),
        DagGraph::new(LeveldbNodeStorage::new(db)),
    )
    .with_merge_policy(Box::new(Changed(seen.clone())));
    (repo, dir, seen)
}
fn op(g: Cid, kind: OperationType<String>, parents: Vec<Cid>, t: u64) -> Operation<Cid, String> {
    let mut op = Operation::new(g, kind, "replica".into());
    op.parents = parents;
    op.timestamp = t;
    op.node_timestamp = Some(t);
    op
}
fn assert_merge_waits_for_parents(partial_multi_parent: bool) {
    let (mut complete, _d1, seen_complete) = repo();
    let (mut incomplete, _d2, seen_incomplete) = repo();
    let placeholder = crsl_lib::dasl::node::Node::new_genesis(
        "root".to_string(),
        1,
        ContentMetadata::with_policy("changed"),
    )
    .content_id()
    .unwrap();
    let mut create = op(placeholder, OperationType::Create("root".into()), vec![], 1);
    create.node_metadata = Some(ContentMetadata::with_policy("changed"));
    let g = complete.commit_operation(create.clone()).unwrap();
    assert_eq!(g, incomplete.commit_operation(create).unwrap());
    let parent = op(g, OperationType::Update("old".into()), vec![g], 2);
    let p = complete.commit_operation(parent.clone()).unwrap();
    let parents = if partial_multi_parent {
        vec![g, p]
    } else {
        vec![p]
    };
    let copy = op(g, OperationType::Update("old".into()), parents, 4);
    let edit = op(g, OperationType::Update("new".into()), vec![g], 3);
    for operation in [copy, edit] {
        assert_eq!(
            complete.commit_operation(operation.clone()).unwrap(),
            incomplete.commit_operation(operation).unwrap()
        );
    }
    let mut h1 = complete.heads(&g).unwrap();
    let mut h2 = incomplete.heads(&g).unwrap();
    h1.sort();
    h2.sort();
    assert_eq!(h1, h2);
    assert_eq!(h1.len(), 2);
    assert!(incomplete.dag.get_node(&p).unwrap().is_none());
    let good = complete.merge_heads(&g).unwrap().unwrap();
    let original_ops = incomplete.state.get_operations_by_genesis(&g).unwrap();
    let error = incomplete
        .merge_heads(&g)
        .expect_err("missing parent must defer merge");
    assert!(error.to_string().contains(&p.to_string()));
    assert!(
        seen_incomplete.lock().unwrap().is_empty(),
        "policy must not see partial inputs"
    );
    let mut remaining = incomplete.heads(&g).unwrap();
    remaining.sort();
    assert_eq!(remaining, h2);
    assert_eq!(
        incomplete.state.get_operations_by_genesis(&g).unwrap(),
        original_ops
    );

    // A failed explicit merge must release its batch, and the lazy path must
    // enforce the same rule without persisting its caller's update either.
    let update = Operation::new(
        g,
        OperationType::Update("after".to_string()),
        "local".into(),
    );
    assert!(incomplete.commit_operation(update).is_err());
    assert_eq!(
        incomplete.state.get_operations_by_genesis(&g).unwrap(),
        original_ops
    );
    assert!(seen_incomplete.lock().unwrap().is_empty());

    assert_eq!(incomplete.commit_operation(parent).unwrap(), p);
    let merged = incomplete.merge_heads(&g).unwrap().unwrap();
    let expected = complete
        .dag
        .get_node(&good)
        .unwrap()
        .unwrap()
        .payload()
        .clone();
    assert_eq!(expected, "new");
    assert_eq!(
        incomplete.dag.get_node(&merged).unwrap().unwrap().payload(),
        &expected
    );
    assert_eq!(incomplete.heads(&g).unwrap(), vec![merged]);
    assert_eq!(incomplete.merge_heads(&g).unwrap(), Some(merged));
    let mut complete_inputs = seen_complete.lock().unwrap().clone();
    let mut retried_inputs = seen_incomplete.lock().unwrap().clone();
    complete_inputs.sort();
    retried_inputs.sort();
    assert_eq!(complete_inputs, retried_inputs);
}
#[test]
fn merge_waits_for_missing_parent() {
    assert_merge_waits_for_parents(false);
}
#[test]
fn merge_waits_for_one_of_two_parents() {
    assert_merge_waits_for_parents(true);
}

fn assert_lww_allows_missing_parents(installed: bool) {
    use crsl_lib::convergence::policies::lww::LwwMergePolicy;
    use crsl_lib::dasl::node::Node;

    let (original, _dir, _) = repo();
    let mut repo = Repo::new(original.state, original.dag);
    if installed {
        repo = repo.with_merge_policy(Box::new(LwwMergePolicy));
    }
    let root = Node::new_genesis("root".to_string(), 1, ContentMetadata::default());
    let genesis = root.content_id().unwrap();
    repo.commit_operation(op(genesis, OperationType::Create("root".into()), vec![], 1))
        .unwrap();
    let missing = Node::new_child(
        "missing".to_string(),
        vec![genesis],
        genesis,
        2,
        ContentMetadata::default(),
    )
    .content_id()
    .unwrap();
    repo.commit_operation(op(
        genesis,
        OperationType::Update("older".into()),
        vec![genesis],
        3,
    ))
    .unwrap();
    repo.commit_operation(op(
        genesis,
        OperationType::Update("newer".into()),
        vec![missing],
        4,
    ))
    .unwrap();
    assert!(repo.dag.get_node(&missing).unwrap().is_none());
    let merged = repo.merge_heads(&genesis).unwrap().unwrap();
    assert_eq!(
        repo.dag.get_node(&merged).unwrap().unwrap().payload(),
        "newer"
    );
    assert_eq!(repo.heads(&genesis).unwrap(), vec![merged]);
}

#[test]
fn named_lww_allows_missing_parents() {
    assert_lww_allows_missing_parents(false);
}

#[test]
fn installed_lww_allows_missing_parents() {
    assert_lww_allows_missing_parents(true);
}
