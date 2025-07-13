# Phase 1: CLIの基盤修正 - 実装タスク

## 1. 依存関係の整理

**ファイル:** `examples/cli.rs`

**作業内容:**
- 不要なインポートを削除（MockStorage関連）
- 必要なインポートを追加

```rust
use crsl_lib::repo::Repo;
use crsl_lib::crdt::{
    crdt_state::CrdtState,
    operation::{Operation, OperationType},
    storage::LeveldbStorage,
};
use crsl_lib::graph::{
    dag::DagGraph,
    storage::LeveldbNodeStorage,
};
use crsl_lib::dasl::cid::ContentId;
use std::error::Error;
use std::path::{Path, PathBuf};
```

## 2. MockStorageの削除

**作業内容:**
- `MockStorage` 構造体とその実装を完全に削除
- `NodeStorage` トレイトの実装も削除
- 関連する型エイリアスも削除

## 3. CLIコマンドの更新

**作業内容:**
- `Commands` enumを以下のように変更：

```rust
#[derive(Subcommand, Clone)]
enum Commands {
    Init {
        #[arg(short, long, default_value = "./crsl_data")]
        path: PathBuf,
    },
    Create {
        #[arg(short, long)]
        content: String,
        #[arg(short, long)]
        author: Option<String>,
    },
    Update {
        #[arg(short, long)]
        genesis_id: String,
        #[arg(short, long)]
        content: String,
        #[arg(short, long)]
        author: Option<String>,
    },
    Show {
        content_id: String,
    },
}
```

## 4. リポジトリの初期化処理

**作業内容:**
- `init` コマンドの実装を更新：

```rust
Commands::Init { path } => {
    // ディレクトリ作成
    std::fs::create_dir_all(&path)?;
    std::fs::create_dir_all(path.join("ops"))?;
    std::fs::create_dir_all(path.join("nodes"))?;
    
    // マーカーファイル作成
    std::fs::write(path.join(".crsl"), "")?;
    
    println!("Initialized CRSL repository at {path:?}");
}
```

## 5. main関数の更新とRepo作成

**作業内容:**
- main関数を以下のように更新：

```rust
fn main() -> Result<(), Box<dyn Error>> {
    let cli = Cli::parse();
    
    match cli.cmd {
        Commands::Init { path } => {
            // initの処理（上記参照）
        }
        other_command => {
            // デフォルトのリポジトリパス
            let repo_path = Path::new("./crsl_data");
            
            // リポジトリが存在するか確認
            if !repo_path.join(".crsl").exists() {
                eprintln!("Repository not found. Run 'init' first.");
                return Ok(());
            }
            
            // Repoを開く
            let op_storage = LeveldbStorage::open(repo_path.join("ops"))?;
            let node_storage = LeveldbNodeStorage::open(repo_path.join("nodes"));
            let state = CrdtState::new(op_storage);
            let dag = DagGraph::new(node_storage);
            let mut repo = Repo::new(state, dag);
            
            match other_command {
                Commands::Create { content, author } => {
                    // Createコマンドの処理
                }
                Commands::Update { genesis_id, content, author } => {
                    // Updateコマンドの処理
                }
                Commands::Show { content_id } => {
                    // Showコマンドの処理
                }
                _ => unreachable!(),
            }
        }
    }
    
    Ok(())
}
```

## 6. Createコマンドの実装

**作業内容:**
- DAG直接操作からRepo経由のCRDT操作に変更：

```rust
Commands::Create { content, author } => {
    // ContentIdを生成
    let content_id_result = ContentId::new(content.as_bytes())?;
    let cid = content_id_result.0;  // 内部のCidを取得
    
    // authorのデフォルト値を設定
    let author = author.unwrap_or_else(|| "anonymous".to_string());
    
    // CRDT操作を作成
    let op = Operation::new(
        cid,
        OperationType::Create(content.clone()),
        author,
    );
    
    // Repoにコミット
    let version_cid = repo.commit_operation(op)?;
    
    println!("Created content:");
    println!("  Content ID: {cid}");
    println!("  Version: {version_cid}");
}
```

## 7. Updateコマンドの実装

**作業内容:**
- 既存のコンテンツを更新する操作を実装：

```rust
Commands::Update { genesis_id, content, author } => {
    // genesis_idをCidにパース
    let genesis_cid = Cid::try_from(genesis_id.as_str())?;
    
    // authorのデフォルト値を設定
    let author = author.unwrap_or_else(|| "anonymous".to_string());
    
    // CRDT操作を作成（genesisを指定）
    let op = Operation::new_with_genesis(
        genesis_cid,  // targetはgenesisと同じ
        genesis_cid,  // genesis
        OperationType::Update(content.clone()),
        author,
    );
    
    // Repoにコミット
    let version_cid = repo.commit_operation(op)?;
    
    println!("Updated content:");
    println!("  Genesis ID: {genesis_id}");
    println!("  Version: {version_cid}");
}
```

## 8. Showコマンドの実装

**作業内容:**
- DAGのget()からCRDTのget_state()に変更：

```rust
Commands::Show { content_id } => {
    // 文字列からCidをパース
    let cid = Cid::try_from(content_id.as_str())?;
    
    // CRDTの状態を取得
    match repo.state.get_state(&cid) {
        Some(content) => {
            println!("Content ID: {content_id}");
            println!("Content: {content}");
            
            // 最新バージョンも表示
            if let Some(latest_version) = repo.latest(&cid) {
                println!("Latest version: {latest_version}");
            }
        }
        None => {
            println!("Content not found: {content_id}");
        }
    }
}
```

## 9. CRDTの設計変更

**重要な変更点:**
- **親のCID指定は不要**: CRDTが自動的に管理
- **genesis_id**: Update操作で元のコンテンツIDを指定
- **author**: オプショナル（デフォルト: "anonymous"）
- **操作の種類**: Create（新規作成）とUpdate（更新）を明確に分離

## 実装の順序

1. MockStorage関連の削除（タスク2）
2. 依存関係の整理（タスク1）
3. コマンドの更新（タスク3）
4. main関数の基本構造（タスク5）
5. 各コマンドの実装（タスク4, 6, 7, 8）
6. 動作確認とデバッグ

これで全ての変更点がカバーされています。エラーハンドリングは含めていません。