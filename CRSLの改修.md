# CRSLの改修

# 目的

CRSLの改修の詳細設計を書いていく。目的としては因果関係を表現可能とし、内容の収束のためのマージポリシーを選択可能とする。

# 詳細設計

## 全体アーキテクチャ

```json
┌─────────────────────────────────────────────────────────┐
│                   Application Layer                     │
│                  (ユーザーアプリケーション)                 │
└────────────────────┬────────────────────────────────────┘
                     │
┌────────────────────▼────────────────────────────────────┐
│                Repository Layer (Repo)                  │
│  - 高レベルAPI提供                                        │
│  - オペレーションの調整                                    │
│  - 自動マージのトリガー                                    │
└─────┬──────────────────────────────┬───────────────────┘
      │                              │
      │                              │
┌─────▼──────────────────┐  ┌────────▼──────────────────┐
│  CRDT State Layer      │  │ Convergence Layer [新規]   │
│  - 操作の記録            │  │ - マージポリシー管理         │
│  - LWW Reducer         │  │ - ContentMetadata [移動]   │
│  [既存: 変更なし]        │  │ - 衝突解決ロジック           │
└─────┬──────────────────┘  └────────┬──────────────────┘
      │                              │
      │                              │
┌─────▼──────────────────────────────▼───────────────────┐
│                    Graph Layer (DAG)                    │
│  - DAG構造の管理                                         │
│  - サイクル検知                                          │
│  - ノードの追加・取得                                     │
│  - 同期機能 [新規: graph/sync.rs]                        │
└─────┬──────────────────────────────────────────────────┘
      │
┌─────▼────────────────────────────────────────────────────┐
│                    DASL Layer                            │
│  - Node<P, M> (汎用構造)                                  │ 
│  - CID生成                                               │
│  [変更なし]                                               │
└──────────────────────────────────────────────────────────┘
```

## レイヤー別設計について

### DASL Layer

変更なし

### Graph Layer

以下変更

- 関数名の変更
    - `fn add_version_node()` → `fn add_child_node()`
    - `fn get_all_versions_for_genesis` → `fn get_nodes_by_genesis`
    - ここでVersionって言葉を使うのは良くない。DAGは因果関係の表現であって、バージョンの表現(線形)ではないため
- 関数を削除
    - `fn get_history_from_version`
- versionって言葉の撤廃
    - 引数や変数名として使用している部分の修正

以下関数

```rust
impl DagGraph {
    // ノード追加
    fn add_genesis_node(&mut self, payload: P, metadata: M) -> Result<Cid>
    fn add_child_node(&mut self, payload: P, parents: Vec<Cid>, genesis: Cid, metadata: M) -> Result<Cid>
    /// ノードを取得
    pub fn get_node(&self, cid: &Cid) -> Result<Option<Node<P, M>>>;
    // ノード取得
    fn get_nodes_by_genesis(&self, genesis: &Cid) -> Result<Vec<Cid>>
    
    // 構造解析
    fn collect_leaf_nodes(&self, nodes: &[Cid]) -> Result<Vec<(Cid, u64)>>
    fn collect_nodes_with_children(&self, nodes: &[Cid]) -> Result<HashSet<Cid>>
    fn calculate_latest(&self, genesis_id: &Cid) -> Result<Option<Cid>>
    fn get_genesis(&self, node_cid: &Cid) -> Result<Cid>
    
    // サイクル検知
    fn would_create_cycle_with(&mut self, new_cid: &Cid, parents: &[Cid]) -> Result<bool>
}
```

### CRDT Layer

- mergeを追加
    - merge操作の場合は、mergeとして識別可能にする

```rust
pub enum OperationType<T> {
    Create(T),
    Update(T),
    Delete,
    Merge(T),  // 追加
}
```

- `target`の削除
    - targetにより、1つ前の状態を表現していたが、DAGにより1つ前の状態を表現できており冗長だったため、削除
    - 引数から削除
    - `fn load_operations_by_genesis()`の削除
    - 以下の構造体に変更

```rust
pub struct Operation<ContentId, T> {
    pub id: OperationId,
    pub genesis: ContentId,
    pub kind: OperationType<T>,
    pub timestamp: Timestamp,
    pub author: Author,
}
```

## Convergence Layer

新しく追加するレイヤー: マージポリシーを持つ。また、ContentMetadataの構造体も持つ。

**全体構成**

```json
src/convergence/
├── mod.rs
├── metadata.rs      # ContentMetadata定義
├── policy.rs        # MergePolicy trait, ResolveInput
├── resolver.rs      # ConflictResolver
└── policies/
    ├── mod.rs
    └── lww.rs       # LwwMergePolicy実装
```

**metadataの定義:**

```rust
// src/convergence/metadata.rs

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ContentMetadata {
    /// ポリシータイプ名（例: "lww", "text", "custom-policy"）
    /// Noneの場合はデフォルト（"lww"）
    policy_type: Option<String>,
}

impl ContentMetadata {
    /// デフォルトのメタデータ（LWWポリシー）
    pub fn new() -> Self {
        Self { policy_type: None }
    }
    
    /// 特定のポリシーを指定
    pub fn with_policy(policy_type: impl Into<String>) -> Self {
        Self {
            policy_type: Some(policy_type.into()),
        }
    }
    
    /// ポリシータイプを取得（Noneの場合は"lww"）
    pub fn policy_type(&self) -> &str {
        self.policy_type.as_deref().unwrap_or("lww")
    }
}

impl Default for ContentMetadata {
    fn default() -> Self {
        Self::new()
    }
}

```

関数:

- `fn new() -> Self`
- `fn with_policy(policy_type: impl Into<String>) -> Self`
- `fn policy_type(&self) -> &str`

`policy.rs` : 複数のマージポリシーに対応するために抽象化する

trait:

```rust
pub trait MergePolicy<P>: Send + Sync {
    /// 競合する複数ノードを解決して統合ペイロードを生成
    fn resolve(&self, nodes: &[ResolveInput<P>]) -> P;
    
    /// ポリシーの識別名（"lww", "text"など）
    fn name(&self) -> &str;
}

/// 構造体
pub struct ResolveInput<P> {
    pub cid: Cid,
    pub payload: P,
    pub timestamp: u64,
}
```

`resolve.rs` : marge nodeを作成する

```rust
pub struct ConflictResolver<P, M> {
    _marker: PhantomData<(P, M)>,
}
```

関数:

- `fn new() -> Self`
- `fn create_merge_node(&self, heads: &[Cid], dag: &DagGraph<impl NodeStorage<P, M>, P, M>, genesis: Cid, policy: &dyn MergePolicy<P>) -> Result<Node<P, M>>`
- `fn collect_inputs(&self, heads: &[Cid], dag: &DagGraph<impl NodeStorage<P, M>, P, M>) -> Result<Vec<ResolveInput<P>>>`

## Repository Layer

構造体の修正:

```rust
pub struct Repo<OpStore, NodeStore, Payload>
where
    OpStore: OperationStorage<Cid, Payload>,
    NodeStore: NodeStorage<Payload, ContentMetadata>,  // ContentMetadata追加
{
    pub state: CrdtState<Cid, Payload, OpStore, LwwReducer>,
    pub dag: DagGraph<NodeStore, Payload, ContentMetadata>,
    resolver: ConflictResolver<Payload, ContentMetadata>,  // 追加
}
```

初期化に引数を追加:

```rust
pub fn new(
    state: CrdtState<Cid, Payload, OpStore, LwwReducer>,
    dag: DagGraph<NodeStore, Payload, ContentMetadata>,
) -> Self {
    Self {
        state,
        dag,
        resolver: ConflictResolver::new(),
    }
}
```

`commit_operation()` にトリガーを追加:

- `let metadata = ContentMetadata::default();`

`commit_operation()`の修正:

- Mergeのハンドリングを追加
- 一旦はユーザーからは受け付けない
    - ただ、手動マージも可能にする予定a

```rust
/// 内部用：無限ループ防止機能付きcommit
/// 
/// # Arguments
/// * `op` - コミットする操作
/// * `skip_auto_merge` - trueの場合、自動マージをスキップ（Merge操作時に使用）
fn commit_operation_internal(
    &mut self, 
    mut op: Operation<Cid, Payload>,
    skip_auto_merge: bool
) -> Result<Cid> {
    let metadata = ContentMetadata::default();
    
    let cid = match &op.kind {
        OperationType::Create(payload) => {
            let genesis_cid = self.dag.add_genesis_node(payload.clone(), metadata)?;
            op.genesis = genesis_cid;
            genesis_cid
        }
        OperationType::Update(payload) => {
            let parents = self.get_latest_parents(&op.genesis);
            self.dag.add_child_node(payload.clone(), parents, op.genesis, metadata)?
        }
        OperationType::Delete => {
            let parents = self.get_latest_parents(&op.genesis);
            let ops = self.state.get_operations_by_genesis(&op.genesis)?;
            let last_payload = ops
                .iter()
                .filter(|o| o.payload().is_some())
                .max_by_key(|o| o.timestamp)
                .expect("content must exist")
                .payload()
                .unwrap()
                .clone();
            self.dag.add_child_node(last_payload, parents, op.genesis, metadata)?
        }
        OperationType::Merge(payload) => {
            // Merge専用：複数のheadを親として持つ
            let parents = self.find_heads(&op.genesis)?;
            self.dag.add_child_node(payload.clone(), parents, op.genesis, metadata)?
        }
    };

    self.state.apply(op)?;

    // 自動マージのトリガー（無限ループ防止）
    if !skip_auto_merge {
        self.check_and_merge(&op.genesis)?;
    }

    Ok(cid)
}

/// 公開API
pub fn commit_operation(&mut self, op: Operation<Cid, Payload>) -> Result<Cid> {
    // Merge操作はユーザーから受け付けない
    if matches!(op.kind, OperationType::Merge(_)) {
        return Err(CrdtError::Internal(
            "Merge operations cannot be manually committed".to_string()
        ));
    }
    
    self.commit_operation_internal(op, false)
}

```

`check_and_merge()` の追加:

**役割**:

- orchestration（調整）: 分岐検知、ポリシー選択、結果の記録
- delegation（委譲）: 実際のマージノード生成はResolverに任せる

```rust
/// 分岐を検知して自動マージを実行
fn check_and_merge(&mut self, genesis: &Cid) -> Result<Option<Cid>> {
    // 1. ヘッドを検索（分岐検知）
    let heads = self.find_heads(genesis)?;
    
    // 2. 分岐がなければ何もしない
    if heads.len() <= 1 {
        return Ok(None);
    }
    
    // 3. genesisノードからポリシータイプを取得（ポリシー選択）
    let genesis_node = self.dag.get_node(genesis)?
        .ok_or_else(|| CrdtError::Internal(format!("Genesis not found: {}", genesis)))?;
    let policy_type = genesis_node.metadata().policy_type();
    
    // 4. ポリシーオブジェクトを生成
    let policy = self.create_policy(policy_type)?;
    
    // 5. マージノード生成を委譲（Resolverに実行を任せる）
    let merge_node = self.resolver.create_merge_node(
        &heads,
        &self.dag,
        *genesis,
        policy.as_ref(),
    )?;

    let merge_op = Operation::new(
        *genesis,
        OperationType::Merge(merge_node.payload().clone()),
        "auto-merge".to_string(),
    );
    
    // 7. commit_operationに委譲（書き込み処理は行わない）
    let merge_cid = self.commit_operation_internal(merge_op, true)?;
    
    Ok(Some(merge_cid))
}
```

### 関数の定義
- `find_head()`
- `get_latest_parents()`
- `create_policy()`
```rust
/// ヘッドノード（リーフノード）を検索
/// 
/// # Returns
/// 分岐している場合は複数のCID、そうでなければ1つ以下
fn find_heads(&self, genesis: &Cid) -> Result<Vec<Cid>> {
    let nodes = self.dag.get_nodes_by_genesis(genesis)?;
    let leaf_nodes = self.dag.collect_leaf_nodes(&nodes)?;
    Ok(leaf_nodes.into_iter().map(|(cid, _)| cid).collect())
}

/// 最新の親ノードを取得（通常は1つ、分岐時は複数）
fn get_latest_parents(&self, genesis: &Cid) -> Vec<Cid> {
    self.dag
        .calculate_latest(genesis)
        .ok()
        .flatten()
        .map(|cid| vec![cid])
        .unwrap_or_default()
}

/// ポリシータイプからポリシーオブジェクトを生成
fn create_policy(&self, policy_type: &str) -> Result<Box<dyn MergePolicy<Payload>>> {
    match policy_type {
        "lww" => Ok(Box::new(LwwMergePolicy)),
        _ => Err(CrdtError::Internal(
            format!("Unknown policy type: {}", policy_type)
        ))
    }
}
```


操作履歴例:

```rust
[
    Operation { 
        genesis: cid_a, 
        kind: Create(payload_a), 
        author: "alice" 
    },  // → DAGにcid_aを生成
    
    Operation { 
        genesis: cid_a, 
        kind: Update(payload_b), 
        author: "bob" 
    },  // → DAGにcid_bを生成
    
    Operation { 
        genesis: cid_a, 
        kind: Update(payload_c), 
        author: "bob" 
    },  // → DAGにcid_cを生成（cid_bの子）
    
    Operation { 
        genesis: cid_a, 
        kind: Update(payload_d), 
        author: "carol" 
    },  // → DAGにcid_dを生成（cid_bの子、cid_cと並列）
    
    Operation { 
        genesis: cid_a, 
        kind: Merge(payload_merged), 
        author: "auto-merge" 
    },  // → DAGにcid_eを生成（cid_cとcid_dの子）
]
```

- `fn get_history` は不要かもしれない
    - ちょっと実装しながら考える