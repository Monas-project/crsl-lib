use cid::Cid;
use clap::{Parser, Subcommand, ValueEnum};
use crsl_lib::convergence::metadata::ContentMetadata;
use crsl_lib::crdt::{
    crdt_state::CrdtState,
    operation::{Operation, OperationType},
    storage::LeveldbStorage,
};
use crsl_lib::dasl::cid::ContentId;
use crsl_lib::graph::{dag::DagGraph, storage::LeveldbNodeStorage};
use crsl_lib::repo::Repo;
use crsl_lib::storage::SharedLeveldb;
use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::path::{Path, PathBuf};

type CliRepo =
    Repo<LeveldbStorage<Cid, String>, LeveldbNodeStorage<String, ContentMetadata>, String>;

const DEFAULT_REPO_PATH: &str = "./crsl_data";

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
        #[arg(long)]
        parent: Option<String>,
    },
    Show {
        content_id: String,
    },
    History {
        #[arg(short, long)]
        genesis_id: String,
        #[arg(long, value_enum, default_value_t = HistoryMode::Tree)]
        mode: HistoryMode,
    },
}

fn main() -> Result<(), Box<dyn Error>> {
    let cli = Cli::parse();

    match cli.cmd {
        Commands::Init { path } => {
            std::fs::create_dir_all(&path)?;
            std::fs::create_dir_all(path.join("store"))?;

            std::fs::write(path.join(".crsl"), "")?;

            println!("Initialized CRSL repository at {path:?} (single LevelDB store)");
        }
        other_command => {
            let repo_path = Path::new(DEFAULT_REPO_PATH);

            if !repo_path.join(".crsl").exists() {
                eprintln!("Repository not found. Run 'init' first.");
                return Ok(());
            }

            let mut repo = open_repo(repo_path)?;

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
                }
                Commands::Update {
                    genesis_id,
                    content,
                    author,
                    parent,
                } => {
                    let author = author.unwrap_or_else(|| "anonymous".to_string());
                    let genesis_cid = Cid::try_from(genesis_id.as_str())?;

                    let mut op = Operation::new(
                        genesis_cid,
                        OperationType::Update(content.clone()),
                        author.clone(),
                    );

                    if let Some(parent) = parent {
                        let parent_cid = Cid::try_from(parent.as_str())?;
                        op.parents.push(parent_cid);
                        println!("📝 Branched update:");
                        println!("   Parent Version: {parent_cid}");
                    } else {
                        println!("📝 Updated content:");
                    }

                    let version_cid = repo.commit_operation(op)?;
                    println!("   Genesis ID: {genesis_id}");
                    println!("   New Version: {version_cid}");

                    if let Some(latest) = repo.latest(&genesis_cid) {
                        if latest == version_cid {
                            println!("   ✅ This is now the latest head");
                        } else {
                            println!("   ℹ️  Latest head remains: {latest}");
                        }
                    }
                }
                Commands::Show { content_id } => {
                    let cid = Cid::try_from(content_id.as_str())?;

                    // First try to get content from CRDT state
                    let content = repo.state.get_state(&cid);

                    // Determine the genesis ID
                    let genesis_cid = if content.is_some() {
                        cid // Content ID is the genesis for CRDT-managed content
                    } else {
                        // For DAG-only nodes, get the genesis from the DAG
                        match repo.get_genesis(&cid) {
                            Ok(genesis) => genesis,
                            Err(_) => cid, // Fallback to CID if genesis lookup fails
                        }
                    };

                    match content {
                        Some(content) => {
                            println!("📄 Content details:");
                            println!("   Content ID: {content_id}");
                            println!("   Content: {content}");
                            println!("   Genesis: {genesis_cid}");

                            // Show relationship between requested and latest version
                            if cid != genesis_cid {
                                println!("   Requested version: {cid} (child of genesis)");
                            } else {
                                println!("   Requested version: {cid} (genesis)");
                            }

                            // Get and display latest version
                            if let Some(latest_version) = repo.latest(&genesis_cid) {
                                if latest_version == cid {
                                    println!("   Latest version: {latest_version} ✅ (this is the latest)");
                                } else {
                                    println!("   Latest version: {latest_version} ⚠️  (this is not the latest)");
                                }
                            } else {
                                println!("   Latest version: Not found");
                            }
                        }
                        None => {
                            // Content not found in CRDT state, but might exist in DAG
                            println!("📄 Content details:");
                            println!("   Content ID: {content_id}");
                            println!("   Content: Not found in CRDT state");
                            println!("   Genesis: {genesis_cid}");
                            println!("   Requested version: {cid} (DAG-only node)");

                            // Try to get latest version from DAG
                            if let Some(latest_version) = repo.latest(&genesis_cid) {
                                if latest_version == cid {
                                    println!("   Latest version: {latest_version} ✅ (this is the latest)");
                                } else {
                                    println!("   Latest version: {latest_version} ⚠️  (this is not the latest)");
                                }
                            } else {
                                println!("   Latest version: Not found");
                            }
                        }
                    }
                }
                Commands::History { genesis_id, mode } => {
                    let genesis_cid = Cid::try_from(genesis_id.as_str())?;
                    let result = match mode {
                        HistoryMode::Tree => display_branching_history(&repo, &genesis_cid),
                        HistoryMode::Linear => display_linear_history(&repo, &genesis_cid),
                    };

                    if let Err(e) = result {
                        eprintln!("❌ Error rendering history: {e}");
                    }
                }
                Commands::Init { .. } => unreachable!("init should be handled before repo setup"),
            }
        }
    }

    Ok(())
}

fn open_repo(repo_path: &Path) -> Result<CliRepo, Box<dyn Error>> {
    let shared = SharedLeveldb::open(repo_path.join("store"))?;
    let state = CrdtState::new(LeveldbStorage::new(shared.clone()));
    let dag = DagGraph::new(LeveldbNodeStorage::new(shared));
    Ok(Repo::new(state, dag))
}

#[derive(Copy, Clone, Debug, ValueEnum)]
enum HistoryMode {
    Tree,
    Linear,
}

fn display_branching_history(repo: &CliRepo, genesis: &Cid) -> Result<(), Box<dyn Error>> {
    let adjacency = repo
        .branching_history(genesis)
        .map_err(Box::<dyn Error>::from)?;
    println!("📜 Branching history for genesis: {genesis}");

    let mut visited = HashSet::new();
    let mut counter = 1;
    print_branching_node(
        repo,
        &adjacency,
        genesis,
        "",
        true,
        &mut visited,
        &mut counter,
    )?;
    Ok(())
}

fn print_branching_node(
    repo: &CliRepo,
    adjacency: &HashMap<Cid, Vec<Cid>>,
    current: &Cid,
    prefix: &str,
    is_last: bool,
    visited: &mut HashSet<Cid>,
    counter: &mut usize,
) -> Result<(), crsl_lib::crdt::error::CrdtError> {
    if !visited.insert(*current) {
        return Ok(());
    }

    let node = repo
        .dag
        .get_node(current)
        .map_err(crsl_lib::crdt::error::CrdtError::Graph)?;

    let (marker, detail) = match node {
        Some(ref n) => {
            let marker = if n.parents().is_empty() {
                "🌱"
            } else if n.parents().len() > 1 {
                "🔀"
            } else {
                "🧩"
            };
            let summary = clean_payload_summary(n.payload());
            let label = format!("node{}", *counter);
            *counter += 1;
            (marker, format!("{label}: {current} | {summary}"))
        }
        None => {
            let label = format!("node{}", *counter);
            *counter += 1;
            ("❓", format!("{label}: {current} (missing)"))
        }
    };

    let branch_symbol = if prefix.is_empty() {
        ""
    } else if is_last {
        "└── "
    } else {
        "├── "
    };
    println!("{prefix}{branch_symbol}{marker} {detail}");

    let mut children: Vec<(Cid, u64)> = adjacency
        .get(current)
        .cloned()
        .unwrap_or_default()
        .into_iter()
        .map(|cid| {
            let ts = repo
                .dag
                .get_node(&cid)
                .map_err(crsl_lib::crdt::error::CrdtError::Graph)?
                .map(|n| n.timestamp())
                .unwrap_or(0);
            Ok::<(Cid, u64), crsl_lib::crdt::error::CrdtError>((cid, ts))
        })
        .collect::<Result<_, _>>()?;

    children.sort_by_key(|(_, ts)| *ts);
    children.dedup_by(|a, b| a.0 == b.0);
    let total = children.len();

    for (index, (child, _)) in children.into_iter().enumerate() {
        let child_is_last = index + 1 == total;
        let new_prefix = if prefix.is_empty() {
            if is_last {
                "    ".to_string()
            } else {
                "│   ".to_string()
            }
        } else if is_last {
            format!("{prefix}    ")
        } else {
            format!("{prefix}│   ")
        };
        print_branching_node(
            repo,
            adjacency,
            &child,
            &new_prefix,
            child_is_last,
            visited,
            counter,
        )?;
    }

    Ok(())
}

fn display_linear_history(repo: &CliRepo, genesis: &Cid) -> Result<(), Box<dyn Error>> {
    let mut path = repo
        .linear_history(genesis)
        .map_err(Box::<dyn Error>::from)?;

    path.dedup();

    if path.is_empty() {
        println!("(no timeline entries for genesis {genesis})");
        return Ok(());
    }

    println!("🧭 Timeline for genesis: {genesis}");
    for (index, cid) in path.iter().enumerate() {
        let node = repo
            .dag
            .get_node(cid)
            .map_err(crsl_lib::crdt::error::CrdtError::Graph)?;
        let (marker, info) = match node {
            Some(ref n) => {
                let marker = if index == 0 {
                    "🌱"
                } else if n.parents().len() > 1 {
                    "🔀"
                } else if index == path.len() - 1 {
                    "✨"
                } else {
                    "🧩"
                };
                let summary = clean_payload_summary(n.payload());
                (marker, format!("node{}: {cid} | {summary}", index + 1))
            }
            None => ("❓", format!("node{}: {cid} (missing)", index + 1)),
        };
        println!("   {marker} {info}");
    }

    Ok(())
}

fn clean_payload_summary(payload: &str) -> String {
    let trimmed = payload.trim();
    if trimmed.len() <= 48 {
        trimmed.to_string()
    } else {
        format!("{}…", &trimmed[..45])
    }
}
