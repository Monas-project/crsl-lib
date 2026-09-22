# CRSL
CRSL is a Rust library for content versioning and CRDT (Conflict-free Replicated Data Type) in distributed systems. It provides content history management, branching, and merging capabilities, supporting offline-capable distributed application development.

## 🚀 Key Features

- **Content Versioning**: Content creation, update, deletion, and history management
- **CRDT Support**: Conflict resolution through Last-Write-Wins (LWW) reducer
- **Auto-Merge**: Automatic conflict resolution when multiple heads exist
- **DAG (Directed Acyclic Graph)**: Efficient version history management
- **LevelDB Storage**: High-performance persistent storage
- **Thread-Safe**: Safe to use in async/await environments with `Mutex`-based storage
- **CID (Content Identifier)**: IPFS-compatible content identifiers

## 🛠️ Usage

### Basic Content Versioning

```rust
use crsl_lib::{
    crdt::{
        crdt_state::CrdtState,
        operation::{Operation, OperationType},
        reducer::LwwReducer,
        storage::LeveldbStorage as OpStore,
    },
    graph::{dag::DagGraph, storage::LeveldbNodeStorage as NodeStorage},
    repo::Repo,
    storage::SharedLeveldb,
};
use tempfile::tempdir;
use cid::Cid;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
struct Content(String);

fn main() {
    // Initialize storage
    let tmp = tempdir().expect("tmp dir");
    let shared = SharedLeveldb::open(tmp.path().join("store")).unwrap();
    let state = CrdtState::new(OpStore::new(shared.clone()));
    let dag = DagGraph::new(NodeStorage::new(shared));
    let mut repo = Repo::new(state, dag);

    // Create a content ID (in practice, you'd use a proper CID)
    let content_id = Cid::new_v1(
        0x55,
        multihash::Multihash::<64>::wrap(0x12, b"content1").unwrap(),
    );
    
    // 1. Create content
    let create_op = Operation::new(
        content_id.clone(),
        OperationType::Create(Content("Initial content".to_string())),
        "user1".to_string(),
    );
    let genesis_cid = repo.commit_operation(create_op).unwrap();

    // 2. Update content
    let update_op = Operation::new(
        genesis_cid,
        OperationType::Update(Content("Updated content".to_string())),
        "user1".to_string(),
    );
    let version_cid = repo.commit_operation(update_op).unwrap();

    // 3. Get history
    let history = repo.get_history(&genesis_cid).unwrap();
    println!("Version history: {:?}", history);
    
    // 4. Get latest version
    if let Some(latest) = repo.latest(&genesis_cid) {
        println!("Latest version: {:?}", latest);
    }
}
```

### Application-supplied merge policies

`Repo::with_merge_policy` supplies an implementation of `MergePolicy` for a
named custom policy. A locally created history records its selected policy in
the genesis metadata. Subsequent automatic merges, including `merge_heads`,
select the implementation named by that genesis; installing a different custom
policy does not override an existing history's choice. Histories using `lww`
continue to use the built-in LWW implementation. An unavailable custom policy
causes a merge error rather than a fallback to a different rule.

The application owns the custom policy's semantics and must deploy compatible,
convergent implementations to its replicas. Policy names are identifiers, not
proof that two implementations behave identically: use a new name when changing
semantics. This API does not migrate existing histories to a new policy, nor does
it prove that an imported merge payload was computed correctly.

Parent-dependent policies require all immediate-parent payloads before a merge
can be saved. If a parent has not synced, import it and retry. Policies independent
of parents can opt out with `MergePolicy::requires_parent_payloads`.

Replication must preserve `Operation::node_metadata` as well as the node
timestamp. The metadata is the exact serialized value, not just a policy name;
replacing it with local defaults can change the node's CID. Legacy operations
without this field remain readable. Newly written LevelDB operations use a
versioned storage format: this version reads old records, but older binaries
cannot read the new records. Do not downgrade a database after writing with this
version. Older replicas are not compatible with new custom-policy histories.

## 🖥️ CLI Tool

CRSL includes a command-line interface for easy content management.

### Basic CLI Commands

```bash
# Initialize repository
cargo run --example cli -- init

# Create content
cargo run --example cli -- create -c "Hello, CRSL!" -a "test-user"

# Show content
cargo run --example cli -- show <CONTENT_ID>

# Update content
cargo run --example cli -- update -g <GENESIS_ID> -c "Updated content" -a "test-user"

# Show history
cargo run --example cli -- history -g <GENESIS_ID>
```

## 📁 Project Structure

```
crsl-lib/
├── src/
│   ├── crdt/              # CRDT implementation
│   │   ├── crdt_state.rs  # CRDT state management
│   │   ├── operation.rs   # Operation definitions
│   │   ├── reducer.rs     # LWW reducer
│   │   ├── storage.rs     # Operation storage (thread-safe)
│   │   └── error.rs       # Error definitions
│   ├── convergence/       # Conflict resolution
│   │   ├── resolver.rs    # Merge orchestration
│   │   ├── policy.rs      # MergePolicy trait
│   │   ├── policies/      # Policy implementations
│   │   │   └── lww.rs     # Last-Write-Wins policy
│   │   └── metadata.rs    # Content metadata
│   ├── graph/             # DAG graph implementation
│   │   ├── dag.rs         # DAG graph management
│   │   ├── storage.rs     # Node storage (thread-safe)
│   │   └── error.rs       # Graph errors
│   ├── dasl/              # DASL (Distributed Application Storage Layer)
│   ├── masl/              # MASL (Multi-Agent Storage Layer)
│   └── repo.rs            # Repository management
├── examples/
│   ├── cli.rs             # Command-line interface
│   └── content_versioning.rs  # Content versioning example
└── crsl_data/             # Data directory
```

## 🔧 Development Setup

### Prerequisites

- Rust 1.79
- Cargo

### Local Execution

```bash
# Clone repository
git clone https://github.com/your-username/crsl-lib.git
cd crsl-lib

# Install dependencies
cargo build

# Setup development environment
make dev-setup

# Run tests
make test

# Run example
cargo run --example content_versioning

# Run demo
make demo
```

## 🧪 Testing

```bash
# Run all tests
make test

# Format code
make fmt

# Run linter
make clippy

# Full check (format + lint + test)
make check

# Clean data
make clean-data
```

## 📚 API Documentation

```bash
# Generate documentation
cargo doc --open
```

## 🔍 Key Components

### CRDT State (`src/crdt/crdt_state.rs`)
- Operation application and state management
- Conflict resolution through LWW reducer
- Integration with operation storage

### Convergence (`src/convergence/`)
- **MergePolicy trait**: Customizable merge strategies
- **LwwMergePolicy**: Last-Write-Wins merge implementation
- **ConflictResolver**: Automatic merge node creation

### DAG Graph (`src/graph/dag.rs`)
- DAG management for version history
- Node addition, retrieval, and history tracking
- Head management and branching

### Repository (`src/repo.rs`)
- Integration of CRDT State and DAG Graph
- Operation commit and history management
- Auto-merge when multiple heads exist
- High-level API provision

### Operations (`src/crdt/operation.rs`)
- Create: New content creation
- Update: Content updates
- Delete: Content deletion
- Merge: Automatic merge operations

### Thread Safety
- `LeveldbStorage` and `LeveldbNodeStorage` use `Mutex` internally
- `OperationStorage` and `NodeStorage` traits require `Send + Sync`
- Safe to use with `Arc<Mutex<Repo>>` in async/await environments

## 📄 License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.

---

**CRSL** - Content Repository Storage Library for Distributed Systems 
