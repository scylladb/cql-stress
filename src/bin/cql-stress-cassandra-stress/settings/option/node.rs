use std::{
    fs::File,
    io::{self, BufRead},
    num::NonZeroUsize,
    sync::Arc,
};

use anyhow::{Context, Result};
use scylla::{
    host_filter::{AllowListHostFilter, HostFilter},
    load_balancing::{DefaultPolicy, LoadBalancingPolicy},
};

use crate::settings::{
    param::{types::CommaDelimitedList, ParamsParser, SimpleParamHandle},
    ParsePayload,
};

pub struct NodeOption {
    pub nodes: Vec<String>,
    pub shard_connection_count: NonZeroUsize,
    pub whitelist: bool,
    pub datacenter: Option<String>,
}

impl NodeOption {
    pub const CLI_STRING: &'static str = "-node";

    pub fn description() -> &'static str {
        "Nodes to connect to"
    }

    pub fn parse(cl_args: &mut ParsePayload) -> Result<Self> {
        let params = cl_args.remove(Self::CLI_STRING).unwrap_or_default();
        let (parser, handles) = prepare_parser();
        parser.parse(params)?;
        Self::from_handles(handles)
    }

    pub fn print_help() {
        let (parser, _) = prepare_parser();
        parser.print_help();
    }

    pub fn print_settings(&self) {
        println!("Node:");
        println!("  Nodes: {:?}", self.nodes);
        println!("  Shard connection count: {}", self.shard_connection_count);
        println!("  Is White List: {}", self.whitelist);
        println!("  Datacenter: {:?}", self.datacenter);
    }

    fn from_handles(handles: NodeParamHandles) -> Result<NodeOption> {
        let datacenter = handles.datacenter.get();
        let shard_connection_count = handles.shard_connection_count.get().unwrap();
        let whitelist = handles.whitelist.get().is_some();
        let file = handles.file.get();
        let nodes = handles.nodes.get();

        let nodes = match nodes {
            Some(nodes) => nodes,
            // SAFETY: Parameters are grouped in a way that either `nodes` or `file` is Some.
            // Note that it's never the case that both of them are Some.
            _ => read_nodes_from_file(&file.unwrap())?,
        };

        Ok(Self {
            nodes,
            shard_connection_count,
            whitelist,
            datacenter,
        })
    }

    /// Define a token-aware load balancing policy with a preferred datacenter (if specified).
    pub fn load_balancing_policy(&self) -> Arc<dyn LoadBalancingPolicy> {
        let mut builder = DefaultPolicy::builder().token_aware(true);
        if let Some(datacenter) = &self.datacenter {
            builder = builder.prefer_datacenter(datacenter.to_owned());
        };
        builder.build()
    }

    /// Limit the communication to the specified nodes (if `whitelist` is set).
    pub fn host_filter(&self, port: u16) -> Option<Result<Arc<dyn HostFilter>>> {
        self.whitelist.then(|| -> Result<Arc<dyn HostFilter>> {
            let addrs = self.nodes.iter().map(|ip| (ip.as_ref(), port));
            Ok(Arc::new(
                AllowListHostFilter::new(addrs).context("Failed to prepare host filter")?,
            ))
        })
    }
}

struct NodeParamHandles {
    datacenter: SimpleParamHandle<String>,
    shard_connection_count: SimpleParamHandle<NonZeroUsize>,
    whitelist: SimpleParamHandle<bool>,
    file: SimpleParamHandle<String>,
    nodes: SimpleParamHandle<CommaDelimitedList>,
}

fn prepare_parser() -> (ParamsParser, NodeParamHandles) {
    let mut parser = ParamsParser::new(NodeOption::CLI_STRING);

    let datacenter = parser.simple_param(
        "datacenter=",
        None,
        "Preferred datacenter for the default load balancing policy",
        false,
    );
    let shard_connection_count = parser.simple_param(
        "shard-connection-count=",
        Some("1"),
        "number of connections per shard",
        false,
    );
    let whitelist = parser.simple_param(
        "whitelist",
        None,
        "Limit communications to the provided nodes",
        false,
    );
    let file = parser.simple_param("file=", None, "Node file (one per line)", false);
    let nodes = parser.simple_param(
        "",
        Some("localhost"),
        "comma delimited list of nodes",
        false,
    );

    // $ ./cassandra-stress help -node
    // Usage: -node [datacenter=?] [shard-connection-count=?] [whitelist] []
    //  OR
    // Usage: -node [datacenter=?] [shard-connection-count=?] [whitelist] [file=?]
    parser.group(&[&datacenter, &shard_connection_count, &whitelist, &nodes]);
    parser.group(&[&datacenter, &shard_connection_count, &whitelist, &file]);

    (
        parser,
        NodeParamHandles {
            datacenter,
            shard_connection_count,
            whitelist,
            file,
            nodes,
        },
    )
}

fn read_nodes_from_file(filename: &str) -> Result<Vec<String>> {
    let file = File::open(filename).context("Invalid nodes file")?;
    let buf = io::BufReader::new(file);
    buf.lines()
        // Filter out empty lines.
        .filter(|s| !s.as_ref().is_ok_and(String::is_empty))
        .collect::<Result<Vec<_>, _>>()
        .context("Invalid nodes file")
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;

    use node::NodeOption;

    use crate::settings::option::node;

    use super::prepare_parser;

    #[test]
    fn node_good_params_test() {
        let args = vec!["whitelist", "127.0.0.1,localhost,192.168.0.1"];
        let (parser, handles) = prepare_parser();

        assert!(parser.parse(args).is_ok());

        let params = NodeOption::from_handles(handles).unwrap();
        assert_eq!(None, params.datacenter);
        assert_eq!(NonZeroUsize::new(1).unwrap(), params.shard_connection_count);
        assert!(params.whitelist);
        assert_eq!(vec!["127.0.0.1", "localhost", "192.168.0.1"], params.nodes);
    }

    #[test]
    fn node_bad_params_test() {
        let args = vec!["whitelist", "127.0.0.1,localhost,192.168.0.1,"];
        let (parser, _) = prepare_parser();

        assert!(parser.parse(args).is_err());
    }
}
