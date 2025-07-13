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

                    println!("Created content:");
                    println!("  Content ID: {cid}");
                    println!("  Version: {version_cid}");
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

                    println!("Updated content:");
                    println!("  Genesis ID: {genesis_id}");
                    println!("  Version: {version_cid}");
                }
                Commands::Show { content_id } => {
                    let cid = Cid::try_from(content_id.as_str())?;

                    match repo.state.get_state(&cid) {
                        Some(content) => {
                            println!("Content ID: {content_id}");
                            println!("Content: {content}");

                            if let Some(latest_version) = repo.latest(&cid) {
                                println!("Latest version: {latest_version}");
                            }
                        }
                        None => {
                            println!("Content not found: {content_id}");
                        }
                    }
                }
                _ => unreachable!(),
            }
        }
    }

    Ok(())
}
