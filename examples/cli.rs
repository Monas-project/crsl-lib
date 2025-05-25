use cid::Cid;
use clap::{Parser, Subcommand};
use crsl_lib::dasl::node::Node;
use crsl_lib::graph::dag::DagGraph;
use crsl_lib::graph::error::{GraphError, Result};
use crsl_lib::graph::storage::NodeStorage;
use std::cell::RefCell;
use std::collections::{BTreeMap, HashMap};
use std::path::PathBuf;

// Type aliases to reduce complexity
type Metadata = BTreeMap<String, String>;
type NodeType = Node<String, Metadata>;
type NodeMap = HashMap<Cid, NodeType>;

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
    nodes: RefCell<NodeMap>,
}

impl MockStorage {
    fn new() -> Self {
        if let Ok(data) = std::fs::read_to_string("nodes.json") {
            if let Ok(nodes) = serde_json::from_str(&data) {
                return Self {
                    nodes: RefCell::new(nodes),
                };
            }
        }
        Self {
            nodes: RefCell::new(HashMap::new()),
        }
    }

    // todo: save to persistent storage
    // currently only in memory
}

impl NodeStorage<String, Metadata> for MockStorage {
    fn get(&self, content_id: &Cid) -> Result<Option<NodeType>> {
        Ok(self.nodes.borrow().get(content_id).cloned())
    }

    fn put(&self, node: &NodeType) -> Result<()> {
        let content_id = node.content_id().map_err(GraphError::Dasl)?;
        self.nodes.borrow_mut().insert(content_id, node.clone());
        Ok(())
    }

    fn delete(&self, content_id: &Cid) -> Result<()> {
        self.nodes.borrow_mut().remove(content_id);
        Ok(())
    }

    fn get_node_map(&self) -> Result<HashMap<Cid, Vec<Cid>>> {
        let nodes = self.nodes.borrow();
        let mut node_map = HashMap::new();
        
        for (cid, node) in nodes.iter() {
            node_map.insert(*cid, node.parents().to_vec());
        }
        
        Ok(node_map)
    }
}

fn main() {
    let cli = Cli::parse();

    let storage = MockStorage::new();
    let mut dag = DagGraph::<_, String, Metadata>::new(storage);

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
            Ok(Some(node)) => {
                println!("Payload: {:?}", node.payload());
                println!("Parents: {:?}", node.parents());
            }
            Ok(None) => eprintln!("Node not found: {}", cid),
            Err(err) => eprintln!("Error getting node: {:?}", err),
        },
        Commands::Verify { cid } => {
            if let Ok(Some(node)) = dag.storage.get(&cid) {
                let ok = node.verify_self_integrity(&cid).unwrap();
                println!("Integrity {}", if ok { "OK" } else { "FAIL" });
            } else {
                eprintln!("Node not found: {}", cid);
            }
        }
    }
}
