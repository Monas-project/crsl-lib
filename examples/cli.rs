use cid::Cid;
use clap::{Parser, Subcommand};
use crsl_lib::dasl::node::Node;
use crsl_lib::graph::dag::DagGraph;
use crsl_lib::graph::storage::NodeStorage;
use std::collections::{BTreeMap, HashMap};
use std::path::PathBuf;

#[derive(clap::Parser, Clone)]
struct Cli {
    #[command(subcommand)]
    cmd: Commands,
}

#[derive(Subcommand, Clone)]
enum Commands {
    Init {
        #[arg(short, long)]
        path: Option<PathBuf>,
    },
    Add {
        #[arg(short = 'l', long)]
        payload: String,
        #[arg(short, long)]
        parents: Vec<Cid>,
        #[arg(short, long, value_delimiter = ',')]
        meta: Vec<String>,
    },
    Show {
        cid: Cid,
    },
    Verify {
        cid: Cid,
    },
}

struct MockStorage {
    nodes: HashMap<Cid, Node<String, BTreeMap<String, String>>>,
}

impl MockStorage {
    fn new() -> Self {
        if let Ok(data) = std::fs::read_to_string("nodes.json") {
            if let Ok(nodes) = serde_json::from_str(&data) {
                return Self { nodes };
            }
        }
        Self {
            nodes: HashMap::new(),
        }
    }

    // todo: save to persistent storage
    // currently only in memory
}

impl NodeStorage<String, BTreeMap<String, String>> for MockStorage {
    fn get(&self, content_id: &Cid) -> Option<Node<String, BTreeMap<String, String>>> {
        self.nodes.get(content_id).cloned()
    }

    fn put(&mut self, node: &Node<String, BTreeMap<String, String>>) {
        self.nodes.insert(node.content_id(), node.clone());
    }

    fn delete(&mut self, content_id: &Cid) {
        self.nodes.remove(content_id);
    }
}

fn main() {
    let cli = Cli::parse();

    let storage = MockStorage::new();
    let mut dag = DagGraph::<_, String, BTreeMap<String, String>>::new(storage);

    match cli.cmd {
        Commands::Init { path } => {
            println!(
                "Init at: {:?} (example no-op)",
                path.unwrap_or_else(|| PathBuf::from("."))
            );
        }
        Commands::Add {
            payload,
            parents,
            meta,
        } => {
            let mut metadata = BTreeMap::new();
            for pair in meta {
                if let Some((k, v)) = pair.split_once('=') {
                    metadata.insert(k.to_string(), v.to_string());
                }
            }

            match dag.add_node(payload, parents, metadata) {
                Ok(cid) => println!("Added node with CID: {}", cid),
                Err(err) => eprintln!("Error adding node: {:?}", err),
            }
        }
        Commands::Show { cid } => match dag.storage.get(&cid) {
            Some(node) => {
                println!("Payload: {:?}", node.payload());
                println!("Parents: {:?}", node.parents());
            }
            None => eprintln!("Node not found: {}", cid),
        },
        Commands::Verify { cid } => {
            if let Some(node) = dag.storage.get(&cid) {
                let ok = node.verify_self_integrity(&cid);
                println!("Integrity {}", if ok { "OK" } else { "FAIL" });
            } else {
                eprintln!("Node not found: {}", cid);
            }
        }
    }
}
