# 設計書

## 概要

この機能は、既存のgenesis追跡機能を活用して、永続化なしで最新バージョンを効率的に計算します。DAG構造を分析し、リーフノード（子を持たないノード）を特定することで、任意のコンテンツの最新バージョンを動的に取得します。

## アーキテクチャ

### 全体構成

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   CLI Layer     │    │   Repo Layer    │    │   DAG Layer     │
│                 │    │                 │    │                 │
│ show command    │───▶│ latest()        │───▶│ calculate_latest│
│                 │    │                 │    │                 │
└─────────────────┘    └─────────────────┘    └─────────────────┘
                                │                        │
                                ▼                        ▼
                       ┌─────────────────┐    ┌─────────────────┐
                       │   CRDT State    │    │  Node Storage   │
                       │                 │    │                 │
                       │ get_state()     │    │ get_node_map()  │
                       │                 │    │ get()           │
                       └─────────────────┘    └─────────────────┘
```

### 計算フロー

1. **Genesis関連ノード取得**: 全ノードからgenesis情報でフィルタ
2. **親子関係分析**: 各ノードの親情報から子を持つノードを特定
3. **リーフノード特定**: 子を持たないノードを収集
4. **最新選択**: タイムスタンプで最新のリーフノードを選択

## コンポーネントと インターフェース

### DagGraph の拡張

```rust
impl<S, P, M> DagGraph<S, P, M>
where
    S: NodeStorage<P, M>,
    P: serde::Serialize + serde::de::DeserializeOwned,
    M: serde::Serialize + serde::de::DeserializeOwned,
{
    /// Genesis IDから最新バージョンを計算
    pub fn calculate_latest(&self, genesis_id: &Cid) -> Result<Option<Cid>>;
    
    /// Genesis IDに関連する全ノードを取得
    fn get_all_versions_for_genesis(&self, genesis_id: &Cid) -> Result<Vec<Cid>>;
}
```

### Repo の修正

```rust
impl<OpStore, NodeStore, Payload> Repo<OpStore, NodeStore, Payload> {
    /// 最新バージョンを取得（毎回計算）
    pub fn latest(&self, genesis_id: &Cid) -> Option<Cid>;
    
    /// 既存のcommit_operationは変更なし（headsは使わない）
    pub fn commit_operation(&mut self, op: Operation<Cid, Payload>) -> Result<Cid>;
}
```

### CLI の拡張

```rust
Commands::Show { content_id } => {
    // 基本情報表示
    // 最新バージョン情報追加
    // Genesis情報表示
}
```

## データモデル

### 既存のNode構造（変更なし）

```rust
pub struct Node<P, M> {
    pub payload: P,
    pub parents: Vec<Cid>,
    pub genesis: Option<Cid>,  // ← これを活用
    pub timestamp: u64,
    pub metadata: M,
}
```

## エラーハンドリング

### エラーの種類と対応

1. **Genesis ID不存在**: `Ok(None)` を返す（正常ケース）
2. **ノード取得エラー**: `GraphError` を伝播
3. **リーフノードなし**: `GraphError::Internal` （理論上発生しない）

### エラーハンドリング戦略

```rust
pub fn calculate_latest(&self, genesis_id: &Cid) -> Result<Option<Cid>> {
    // Genesis存在確認（存在しない場合はNone）
    let versions = self.get_all_versions_for_genesis(genesis_id)?;
    if versions.is_empty() {
        return Ok(None);
    }
    
    // 実際のエラーのみ伝播
    // ...
}
```

## テスト戦略

### 単体テスト

1. **DagGraph::calculate_latest()**
   - 単一バージョン（genesis のみ）
   - 線形履歴（複数バージョン）
   - 分岐履歴（複数リーフ）
   - 存在しないgenesis ID

2. **DagGraph::get_all_versions_for_genesis()**
   - Genesis自身の検出
   - 子ノードの検出
   - 無関係ノードの除外

3. **Repo::latest()**
   - 正常ケース
   - エラーケース
   - 空の履歴

### テストデータ

```rust
// テスト用のDAG構造例
// Genesis -> V1 -> V2 -> V3 (linear)
// Genesis -> V1 -> V2a, V2b (branched)
```

## 実装の詳細

### アルゴリズムの実装

```rust
pub fn calculate_latest(&self, genesis_id: &Cid) -> Result<Option<Cid>> {
    // Step 1: Genesis IDに関連する全ノードを取得
    let versions = self.get_all_versions_for_genesis(genesis_id)?;
    
    if versions.is_empty() {
        return Ok(None);
    }
    
    // 単一ノードの場合は即座に返す
    if versions.len() == 1 {
        return Ok(Some(versions[0]));
    }
    
    // Step 2: 親として参照されているノードを特定
    let mut has_children = HashSet::new();
    
    for &node_cid in &versions {
        if let Some(node) = self.storage.get(&node_cid)? {
            for parent_cid in node.parents() {
                if versions.contains(parent_cid) {
                    has_children.insert(*parent_cid);
                }
            }
        }
    }
    
    // Step 3: リーフノード（子を持たないノード）を収集
    let mut leaf_nodes = Vec::new();
    
    for &node_cid in &versions {
        if !has_children.contains(&node_cid) {
            if let Some(node) = self.storage.get(&node_cid)? {
                leaf_nodes.push((node_cid, node.timestamp()));
            }
        }
    }
    
    // Step 4: 最新のリーフノードを返す
    leaf_nodes.sort_by_key(|(_, timestamp)| std::cmp::Reverse(*timestamp));
    Ok(leaf_nodes.first().map(|(cid, _)| *cid))
}
```

### Genesis関連ノード取得の実装

```rust
fn get_all_versions_for_genesis(&self, genesis_id: &Cid) -> Result<Vec<Cid>> {
    let mut versions = Vec::new();
    let node_map = self.storage.get_node_map()?;
    
    // 全ノードを確認してgenesis情報でフィルタ
    for (cid, _) in node_map {
        if let Some(node) = self.storage.get(&cid)? {
            // ノード自身がgenesisか、またはgenesisを参照している
            if cid == *genesis_id || node.genesis == Some(*genesis_id) {
                versions.push(cid);
            }
        }
    }
    
    Ok(versions)
}
```

### パフォーマンス最適化

1. **早期リターン**: 単一ノードの場合は即座に返す
2. **効率的なフィルタリング**: genesis情報による事前フィルタ
3. **メモリ効率**: 一時的なデータ構造のみ使用

## セキュリティ考慮事項

1. **入力検証**: CID形式の検証
2. **リソース制限**: 大量ノードでのメモリ使用量制限
3. **エラー情報**: 内部構造を露出しないエラーメッセージ

## 運用考慮事項

1. **ログ出力**: 計算時間とノード数のログ
2. **メトリクス**: パフォーマンス監視用メトリクス
3. **デバッグ**: 計算過程の可視化機能