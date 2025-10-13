//! examples/content_versioning.rs
//!
//! 1. Create  (v1)
//! 2. Update  (v2)
//! 3. Update  (v3a)  ← branch A
//! 4. Update  (v3b)  ← branch B
//! 5. Merge   (v4)   ← parents = [v3a, v3b]

use crsl_lib::{
    crdt::{
        crdt_state::CrdtState,
        operation::{Operation, OperationType},
        reducer::LwwReducer,
        storage::LeveldbStorage as OpStore,
    },
    graph::{dag::DagGraph, storage::LeveldbNodeStorage as NodeStorage},
};
use tempfile::tempdir;

type Content = String;
type Store = OpStore<String, Content>;
type ContentState = CrdtState<String, Content, Store, LwwReducer>;

fn main() {
    let tmp = tempdir().expect("tmp dir");
    let op_store = OpStore::open(tmp.path().join("ops")).unwrap();
    let node_store = NodeStorage::open(tmp.path().join("nodes"));
    let state = ContentState::new(op_store);
    let mut _dag = DagGraph::<_, Content, ()>::new(node_store);

    let content_id = "content1".to_string();
    let create_op = Operation::new(
        content_id.clone(),
        OperationType::Create("Initial content".to_string()),
        "user1".to_string(),
    );

    // Apply the create operation
    state.apply(create_op).unwrap();

    // ────────────────────────────────────────────────
    // 1. Create  (v1)
    // ────────────────────────────────────────────────
    let cid = "content1".to_owned();
    let create_op = Operation::new(
        cid.clone(),
        OperationType::Create("Initial content".into()),
        "user1".into(),
    );
    state.apply(create_op.clone()).unwrap();
    // todo: implement commit to dag
    // let parent = dag.latest_head(&op.target);
    // dag.add_node(
    //     op.payload().unwrap().clone(),
    //     parent.into_iter().collect(),
    //     (),
    // ).unwrap();

    // ────────────────────────────────────────────────
    // 2. Update  (v2)    ← HEAD = v1
    // ────────────────────────────────────────────────
    // todo: find the latest root_id or maybe get root_id from latest node??
    let op_v2 = Operation::new(
        cid.clone(),
        OperationType::Update("Updated content".into()),
        "user1".into(),
    );
    state.apply(op_v2).unwrap();
    // todo: implement commit to dag
    // let parent = dag.latest_head(&op.target);
    // dag.add_node(
    //     op.payload().unwrap().clone(),
    //     parent.into_iter().collect(),
    //     (),
    // ).unwrap();

    // ────────────────────────────────────────────────
    // 3. Update  (v3a)  ← branch A
    // ────────────────────────────────────────────────
    let op_v3a = Operation::new(
        content_id.clone(),
        OperationType::Update("Updated content 2".to_string()),
        "user2".to_string(),
    );
    state.apply(op_v3a).unwrap();
    // todo: commit to dag
    // let parent = dag.latest_head(&op.target);
    // dag.add_node(
    //     op.payload().unwrap().clone(),
    //     parent.into_iter().collect(),
    //     (),
    // ).unwrap();

    // ────────────────────────────────────────────────
    // 4. Update  (v3b)   ← branch B (parent = v2)
    // ────────────────────────────────────────────────
    let op_v3b = Operation::new(
        content_id.clone(),
        OperationType::Update("Updated content B".into()),
        "userB".into(),
    );
    state.apply(op_v3b).unwrap();
    // todo: commit to dag
    // let parent = dag.latest_head(&op.target);
    // dag.add_node(
    //     op.payload().unwrap().clone(),
    //     parent.into_iter().collect(),
    //     (),
    // ).unwrap();

    // ────────────────────────────────────────────────
    // 6. Show version history
    // ────────────────────────────────────────────────
    // let history = topo_sort(&dag);
    // println!("--- Version history ---");
    // for (i, c) in history.iter().enumerate() {
    //     println!("v{}  {}", i + 1, short(c));
    // }
}
