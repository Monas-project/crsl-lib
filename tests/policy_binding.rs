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
    dasl::node::Node,
    graph::{dag::DagGraph, storage::LeveldbNodeStorage},
    repo::Repo,
    storage::SharedLeveldb,
};
type TestRepo =
    Repo<LeveldbStorage<Cid, String>, LeveldbNodeStorage<String, ContentMetadata>, String>;
struct Custom(&'static str);
impl MergePolicy<String> for Custom {
    fn name(&self) -> &str {
        self.0
    }
    fn resolve(&self, _: &[ResolveInput<String>]) -> String {
        "custom-result".into()
    }
}
fn repo(policy: Option<&'static str>) -> (TestRepo, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let db = SharedLeveldb::open(dir.path()).unwrap();
    let repo = Repo::new(
        CrdtState::new(LeveldbStorage::new(db.clone())),
        DagGraph::new(LeveldbNodeStorage::new(db)),
    );
    (
        match policy {
            Some(name) => repo.with_merge_policy(Box::new(Custom(name))),
            None => repo,
        },
        dir,
    )
}
fn seed() -> Cid {
    Node::new_genesis("seed".to_string(), 1, ContentMetadata::default())
        .content_id()
        .unwrap()
}
#[test]
fn local_create_records_selected_policy() {
    let (mut repo, _dir) = repo(Some("custom-v1"));
    let g = repo
        .commit_operation(Operation::new(
            seed(),
            OperationType::Create("root".into()),
            "local".into(),
        ))
        .unwrap();
    assert_eq!(
        repo.dag
            .get_node(&g)
            .unwrap()
            .unwrap()
            .metadata()
            .policy_type(),
        "custom-v1"
    );
}

#[test]
fn exported_operations_reconstruct_exact_custom_nodes_without_receiver_policy() {
    let (mut source, _dir) = repo(Some("custom-v1"));
    let g = source
        .commit_operation(Operation::new(
            seed(),
            OperationType::Create("root".into()),
            "local".into(),
        ))
        .unwrap();
    branches(&mut source, g);
    source.merge_heads(&g).unwrap();
    let ops = source.get_operations_with_index(&g).unwrap();
    for installed in [None, Some("wrong-v1"), Some("custom-v1")] {
        let (mut receiver, _dir) = repo(installed);
        // Import children before genesis: metadata cannot depend on local ancestry.
        for (_, op) in ops.iter().rev() {
            let json = serde_json::to_vec(op).unwrap();
            let imported = serde_json::from_slice(&json).unwrap();
            let cid = receiver.commit_operation(imported).unwrap();
            assert_eq!(
                receiver.dag.get_node(&cid).unwrap(),
                source.dag.get_node(&cid).unwrap()
            );
            assert!(source.dag.get_node(&cid).unwrap().is_some());
        }
        assert_eq!(receiver.latest(&g), source.latest(&g));
        assert_eq!(
            receiver.dag.get_node(&g).unwrap(),
            source.dag.get_node(&g).unwrap()
        );
    }
}

#[test]
fn legacy_json_import_uses_historical_default_not_installed_policy() {
    let expected = Node::new_genesis("legacy".to_string(), 7, ContentMetadata::default());
    let g = expected.content_id().unwrap();
    let mut op = Operation::new(g, OperationType::Create("legacy".to_string()), "old".into());
    op.node_timestamp = Some(7);
    let mut json = serde_json::to_value(op).unwrap();
    json.as_object_mut().unwrap().remove("node_metadata");
    for installed in [None, Some("custom-v1")] {
        let (mut receiver, _dir) = repo(installed);
        let imported: Operation<Cid, String> = serde_json::from_value(json.clone()).unwrap();
        assert_eq!(imported.node_metadata, None);
        assert_eq!(receiver.commit_operation(imported).unwrap(), g);
        assert_eq!(receiver.dag.get_node(&g).unwrap(), Some(expected.clone()));
    }
}

#[test]
fn explicit_metadata_preserves_representation_and_local_create_choice() {
    for metadata in [
        ContentMetadata::default(),
        ContentMetadata::with_policy("lww"),
        ContentMetadata::with_policy("custom-v1"),
    ] {
        let expected = Node::new_genesis("prepared".to_string(), 7, metadata.clone());
        let g = expected.content_id().unwrap();
        let (mut receiver, _dir) = repo(Some("different"));
        let mut imported = Operation::new(
            g,
            OperationType::Create("prepared".into()),
            "prepared".into(),
        );
        imported.node_metadata = Some(metadata.clone());
        imported.node_timestamp = Some(7);
        assert_eq!(receiver.commit_operation(imported).unwrap(), g);
        assert_eq!(receiver.dag.get_node(&g).unwrap(), Some(expected));
        let mut local = Operation::new(
            seed(),
            OperationType::Create("local".into()),
            "local".into(),
        );
        local.node_metadata = Some(metadata.clone());
        let local_g = receiver.commit_operation(local).unwrap();
        assert_eq!(
            receiver.dag.get_node(&local_g).unwrap().unwrap().metadata(),
            &metadata
        );
    }
}

#[test]
fn builtin_lww_name_is_reserved_even_for_an_installed_policy() {
    let (mut repo, _dir) = repo(Some("lww"));
    let g = repo
        .commit_operation(Operation::new(
            seed(),
            OperationType::Create("root".into()),
            "local".into(),
        ))
        .unwrap();
    branches(&mut repo, g);
    let merged = repo.merge_heads(&g).unwrap().unwrap();
    assert_eq!(
        repo.dag.get_node(&merged).unwrap().unwrap().payload(),
        "newer"
    );
}

#[test]
fn imported_custom_genesis_cannot_lose_metadata_silently() {
    let (mut source, _dir) = repo(Some("custom-v1"));
    let g = source
        .commit_operation(Operation::new(
            seed(),
            OperationType::Create("root".into()),
            "local".into(),
        ))
        .unwrap();
    let mut op = source.get_operations_with_index(&g).unwrap().remove(0).1;
    op.node_metadata = None;
    let (mut receiver, _dir) = repo(Some("custom-v1"));
    assert!(receiver
        .commit_operation(op)
        .unwrap_err()
        .to_string()
        .contains("CID mismatch"));
    assert!(receiver.dag.get_nodes_by_genesis(&g).unwrap().is_empty());
    assert!(receiver
        .state
        .get_operations_by_genesis(&g)
        .unwrap()
        .is_empty());
}

#[test]
fn imported_child_policy_must_match_known_genesis_without_writes() {
    for kind in [
        OperationType::Update("bad".into()),
        OperationType::Merge("bad".into()),
        OperationType::Delete,
    ] {
        let (mut repo, _dir) = repo(Some("custom-v1"));
        let g = repo
            .commit_operation(Operation::new(
                seed(),
                OperationType::Create("root".into()),
                "local".into(),
            ))
            .unwrap();
        let before = repo.state.get_operations_by_genesis(&g).unwrap();
        let mut imported = Operation::new(g, kind, "remote".into());
        imported.parents = vec![g];
        imported.node_timestamp = Some(10);
        imported.node_metadata = Some(ContentMetadata::default());
        assert!(repo
            .commit_operation(imported)
            .unwrap_err()
            .to_string()
            .contains("Policy mismatch"));
        assert_eq!(repo.state.get_operations_by_genesis(&g).unwrap(), before);
        assert_eq!(repo.dag.get_nodes_by_genesis(&g).unwrap(), vec![g]);
    }
}

#[test]
fn heads_imported_before_genesis_are_checked_before_merging() {
    let (mut source, _dir) = repo(Some("custom-v1"));
    let g = source
        .commit_operation(Operation::new(
            seed(),
            OperationType::Create("root".into()),
            "local".into(),
        ))
        .unwrap();
    let create = source.get_operations_with_index(&g).unwrap().remove(0).1;
    let (mut receiver, _dir) = repo(Some("custom-v1"));
    let mut bad_head = None;
    for (t, metadata) in [
        (10, ContentMetadata::default()),
        (11, ContentMetadata::with_policy("custom-v1")),
    ] {
        let mut imported = Operation::new(
            g,
            OperationType::Update(format!("head-{t}")),
            "remote".into(),
        );
        imported.parents = vec![g];
        imported.node_timestamp = Some(t);
        imported.node_metadata = Some(metadata);
        let cid = receiver.commit_operation(imported).unwrap();
        if t == 10 {
            bad_head = Some(cid);
        }
    }
    receiver.commit_operation(create).unwrap();
    let before = receiver.state.get_operations_by_genesis(&g).unwrap();
    assert!(receiver
        .merge_heads(&g)
        .unwrap_err()
        .to_string()
        .contains("Policy mismatch"));
    let mut update = Operation::new(g, OperationType::Update("after".into()), "local".into());
    assert!(receiver
        .commit_operation(update.clone())
        .unwrap_err()
        .to_string()
        .contains("Policy mismatch"));
    update.parents = vec![bad_head.unwrap()];
    assert!(receiver
        .commit_operation(update)
        .unwrap_err()
        .to_string()
        .contains("Policy mismatch"));
    assert_eq!(
        receiver.state.get_operations_by_genesis(&g).unwrap(),
        before
    );
    assert_eq!(receiver.heads(&g).unwrap().len(), 2);
}

fn branches(repo: &mut TestRepo, g: Cid) {
    for value in ["older", "newer"] {
        let mut op = Operation::new(g, OperationType::Update(value.into()), "local".into());
        op.parents = vec![g];
        repo.commit_operation(op).unwrap();
    }
}

#[test]
fn existing_lww_is_not_overridden() {
    for lazy in [false, true] {
        let (mut original, _dir) = repo(None);
        let g = original
            .commit_operation(Operation::new(
                seed(),
                OperationType::Create("root".into()),
                "local".into(),
            ))
            .unwrap();
        branches(&mut original, g);
        let mut installed = original.with_merge_policy(Box::new(Custom("different")));
        if lazy {
            installed
                .commit_operation(Operation::new(
                    g,
                    OperationType::Update("after".into()),
                    "local".into(),
                ))
                .unwrap();
        } else {
            installed.merge_heads(&g).unwrap();
        }
        let ops = installed.state.get_operations_by_genesis(&g).unwrap();
        let merged = ops
            .iter()
            .find_map(|op| match &op.kind {
                OperationType::Merge(p) => Some(p.as_str()),
                _ => None,
            })
            .unwrap();
        assert_eq!(merged, "newer");
    }
}

#[test]
fn unavailable_custom_policy_rejects_merges_without_writes() {
    for installed in [None, Some("wrong-v1")] {
        let (mut original, _dir) = repo(Some("custom-v1"));
        let g = original
            .commit_operation(Operation::new(
                seed(),
                OperationType::Create("root".into()),
                "local".into(),
            ))
            .unwrap();
        branches(&mut original, g);
        let mut receiver = Repo::new(original.state, original.dag);
        if let Some(name) = installed {
            receiver = receiver.with_merge_policy(Box::new(Custom(name)));
        }
        let ops = receiver.state.get_operations_by_genesis(&g).unwrap();
        let nodes: std::collections::HashSet<_> = receiver
            .dag
            .get_nodes_by_genesis(&g)
            .unwrap()
            .into_iter()
            .collect();
        let heads: std::collections::HashSet<_> = receiver.heads(&g).unwrap().into_iter().collect();
        assert!(receiver
            .merge_heads(&g)
            .unwrap_err()
            .to_string()
            .contains("custom-v1"));
        assert!(receiver
            .commit_operation(Operation::new(
                g,
                OperationType::Update("after".into()),
                "local".into()
            ))
            .unwrap_err()
            .to_string()
            .contains("custom-v1"));
        assert_eq!(receiver.state.get_operations_by_genesis(&g).unwrap(), ops);
        assert_eq!(
            receiver
                .dag
                .get_nodes_by_genesis(&g)
                .unwrap()
                .into_iter()
                .collect::<std::collections::HashSet<_>>(),
            nodes
        );
        assert_eq!(
            receiver
                .heads(&g)
                .unwrap()
                .into_iter()
                .collect::<std::collections::HashSet<_>>(),
            heads
        );
        let mut receiver = receiver.with_merge_policy(Box::new(Custom("custom-v1")));
        let merged = receiver.merge_heads(&g).unwrap().unwrap();
        assert_eq!(
            receiver.dag.get_node(&merged).unwrap().unwrap().payload(),
            "custom-result"
        );
    }
}
