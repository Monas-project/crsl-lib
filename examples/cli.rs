use cid::Cid;
use clap::{Parser, Subcommand};
use crsl_lib::crdt::{
    crdt_state::CrdtState,
    operation::{Operation, OperationType},
    storage::LeveldbStorage,
};
use crsl_lib::dasl::cid::ContentId;
use crsl_lib::graph::{dag::DagGraph, storage::LeveldbNodeStorage};
use crsl_lib::repo::Repo;
use std::error::Error;
use std::path::{Path, PathBuf};

#[derive(clap::Parser, Clone)]
struct Cli {
    #[command(subcommand)]
    cmd: Commands,
}

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
    History {
        #[arg(short, long)]
        genesis_id: String,
    },
    HistoryFromVersion {
        #[arg(short, long)]
        version_id: String,
    },
    Genesis {
        #[arg(short, long)]
        version_id: String,
    },
}

fn main() -> Result<(), Box<dyn Error>> {
    let cli = Cli::parse();

    match cli.cmd {
        Commands::Init { path } => {
            std::fs::create_dir_all(&path)?;
            std::fs::create_dir_all(path.join("ops"))?;
            std::fs::create_dir_all(path.join("nodes"))?;

            std::fs::write(path.join(".crsl"), "")?;

            println!("Initialized CRSL repository at {path:?}");
        }
        other_command => {
            let repo_path = Path::new("./crsl_data");

            if !repo_path.join(".crsl").exists() {
                eprintln!("Repository not found. Run 'init' first.");
                return Ok(());
            }

            let op_storage = LeveldbStorage::open(repo_path.join("ops"))?;
            let node_storage = LeveldbNodeStorage::open(repo_path.join("nodes"));
            let state = CrdtState::new(op_storage);
            let dag = DagGraph::new(node_storage);
            let mut repo = Repo::new(state, dag);

            match other_command {
                Commands::Create { content, author } => {
                    let content_id_result = ContentId::new(content.as_bytes())?;
                    let cid = content_id_result.0;

                    let author = author.unwrap_or_else(|| "anonymous".to_string());

                    let op = Operation::new(cid, OperationType::Create(content.clone()), author);

                    let version_cid = repo.commit_operation(op)?;

                    println!("✅ Created content:");
                    println!("   Content ID: {cid}");
                    println!("   Genesis: {version_cid}");
                    println!("   Version: {version_cid}");
                    println!("🔍 Debug: Latest head for genesis {}: {:?}", version_cid, repo.latest(&version_cid));
                }
                Commands::Update {
                    genesis_id,
                    content,
                    author,
                } => {
                    let genesis_cid = Cid::try_from(genesis_id.as_str())?;

                    let author = author.unwrap_or_else(|| "anonymous".to_string());

                    let op = Operation::new_with_genesis(
                        genesis_cid,
                        genesis_cid,
                        OperationType::Update(content.clone()),
                        author,
                    );

                    let version_cid = repo.commit_operation(op)?;

                    println!("📝 Updated content:");
                    println!("   Genesis ID: {genesis_id}");
                    println!("   New Version: {version_cid}");
                }
                Commands::Show { content_id } => {
                    let cid = Cid::try_from(content_id.as_str())?;

                    match repo.state.get_state(&cid) {
                        Some(content) => {
                            println!("📄 Content details:");
                            println!("   Content ID: {content_id}");
                            println!("   Content: {content}");
                            if let Some(latest_version) = repo.latest(&cid) {
                                println!("   Latest version: {latest_version}");
                            }
                        }
                        None => {
                            println!("❌ Content not found: {content_id}");
                        }
                    }
                }
                Commands::History { genesis_id } => {
                    let genesis_cid = Cid::try_from(genesis_id.as_str())?;

                    match repo.get_history(&genesis_cid) {
                        Ok(history) => {
                            println!("📜 History for genesis: {genesis_id}");
                            if history.is_empty() {
                                println!("   No history found (genesis only)");
                            } else {
                                for (i, version_cid) in history.iter().enumerate() {
                                    let marker = if i == 0 {
                                        "🌱"
                                    } else if i == history.len() - 1 {
                                        "✨"
                                    } else {
                                        "📝"
                                    };
                                    println!("   {} {}: {}", marker, i + 1, version_cid);
                                }
                            }
                        }
                        Err(e) => {
                            eprintln!("❌ Error getting history: {e}");
                        }
                    }
                }
                Commands::HistoryFromVersion { version_id } => {
                    let version_cid = Cid::try_from(version_id.as_str())?;

                    match repo.dag.get_history_from_version(&version_cid) {
                        Ok(history) => {
                            println!("📜 History from version: {version_id}");
                            for (i, version_cid) in history.iter().enumerate() {
                                let marker = if i == 0 {
                                    "🌱"
                                } else if i == history.len() - 1 {
                                    "✨"
                                } else {
                                    "📝"
                                };
                                println!("   {} {}: {}", marker, i + 1, version_cid);
                            }
                        }
                        Err(e) => {
                            eprintln!("❌ Error getting history from version: {e}");
                        }
                    }
                }
                Commands::Genesis { version_id } => {
                    let version_cid = Cid::try_from(version_id.as_str())?;

                    match repo.get_genesis(&version_cid) {
                        Ok(genesis_cid) => {
                            println!("🌱 Genesis for version: {version_id}");
                            println!("   Genesis CID: {genesis_cid}");
                        }
                        Err(e) => {
                            eprintln!("❌ Error getting genesis: {e}");
                        }
                    }
                }
                _ => unreachable!(),
            }
        }
    }

    Ok(())
}
