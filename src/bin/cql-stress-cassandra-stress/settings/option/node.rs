use std::{
    fs::File,
    io::{self, BufRead},
    sync::Arc,
};

use crate::settings::{
    param::{types::CommaDelimitedList, ParamsParser, SimpleParamHandle},
    ParsePayload,
};
use anyhow::{Context, Result};
use scylla::policies::host_filter::{AllowListHostFilter, HostFilter};
use scylla::policies::load_balancing::{DefaultPolicy, LoadBalancingPolicy};

pub struct NodeOption {
    pub nodes: Vec<String>,
    pub whitelist: bool,
    pub datacenter: Option<String>,
    pub rack: Option<String>,
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
        println!("  Is White List: {}", self.whitelist);
        println!("  Datacenter: {:?}", self.datacenter);
        println!("  Rack: {:?}", self.rack);
    }

    fn from_handles(handles: NodeParamHandles) -> Result<NodeOption> {
        let datacenter = handles.datacenter.get();
        let rack = handles.rack.get();
        let whitelist = handles.whitelist.get().is_some();
        let file = handles.file.get();
        let nodes = handles.nodes.get();

        if datacenter.as_deref() == Some("") {
            anyhow::bail!("`datacenter` must not be empty");
        }
        if rack.as_deref() == Some("") {
            anyhow::bail!("`rack` must not be empty");
        }
        anyhow::ensure!(
            rack.is_none() || datacenter.is_some(),
            "`rack` requires `datacenter` to also be set"
        );

        let nodes = match nodes {
            Some(nodes) => nodes,
            // SAFETY: Parameters are grouped in a way that either `nodes` or `file` is Some.
            // Note that it's never the case that both of them are Some.
            _ => read_nodes_from_file(&file.unwrap())?,
        };

        Ok(Self {
            nodes,
            whitelist,
            datacenter,
            rack,
        })
    }

    /// Define a token-aware load balancing policy.
    ///
    /// The preferred datacenter/rack are not set on the policy itself; they are
    /// configured on the [`SessionBuilder`](scylla::client::session_builder::SessionBuilder)
    /// (see [`Self::datacenter`] / [`Self::rack`]), which is the recommended way. A
    /// `DefaultPolicy` with no explicit preference falls back to the session's preference.
    pub fn load_balancing_policy(&self) -> Arc<dyn LoadBalancingPolicy> {
        DefaultPolicy::builder().token_aware(true).build()
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
    rack: SimpleParamHandle<String>,
    whitelist: SimpleParamHandle<bool>,
    file: SimpleParamHandle<String>,
    nodes: SimpleParamHandle<CommaDelimitedList>,
}

fn prepare_parser() -> (ParamsParser, NodeParamHandles) {
    let mut parser = ParamsParser::new(NodeOption::CLI_STRING);

    let datacenter = parser.simple_param(
        "datacenter=",
        None,
        "Preferred datacenter, set on the SessionBuilder",
        false,
    );
    let rack = parser.simple_param(
        "rack=",
        None,
        "Preferred rack within the preferred datacenter, set on the SessionBuilder; \
        requires `datacenter`",
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
    // Usage: -node [datacenter=?] [rack=?] [whitelist] []
    //  OR
    // Usage: -node [datacenter=?] [rack=?] [whitelist] [file=?]
    parser.group(&[&datacenter, &rack, &whitelist, &nodes]);
    parser.group(&[&datacenter, &rack, &whitelist, &file]);

    (
        parser,
        NodeParamHandles {
            datacenter,
            rack,
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
        assert_eq!(None, params.rack);
        assert!(params.whitelist);
        assert_eq!(vec!["127.0.0.1", "localhost", "192.168.0.1"], params.nodes);
    }

    #[test]
    fn node_datacenter_and_rack_test() {
        let args = vec!["datacenter=dc1", "rack=rack1", "127.0.0.1"];
        let (parser, handles) = prepare_parser();

        assert!(parser.parse(args).is_ok());

        let params = NodeOption::from_handles(handles).unwrap();
        assert_eq!(Some("dc1".to_owned()), params.datacenter);
        assert_eq!(Some("rack1".to_owned()), params.rack);
    }

    #[test]
    fn node_rack_without_datacenter_test() {
        let args = vec!["rack=rack1", "127.0.0.1"];
        let (parser, handles) = prepare_parser();

        assert!(parser.parse(args).is_ok());
        // `rack` without `datacenter` must be rejected.
        assert!(NodeOption::from_handles(handles).is_err());
    }

    #[test]
    fn node_empty_datacenter_test() {
        let args = vec!["datacenter=", "127.0.0.1"];
        let (parser, handles) = prepare_parser();

        assert!(parser.parse(args).is_ok());
        // empty datacenter string must be rejected
        assert!(NodeOption::from_handles(handles).is_err());
    }

    #[test]
    fn node_empty_rack_test() {
        let args = vec!["datacenter=dc1", "rack=", "127.0.0.1"];
        let (parser, handles) = prepare_parser();

        assert!(parser.parse(args).is_ok());
        // empty rack string must be rejected
        assert!(NodeOption::from_handles(handles).is_err());
    }

    #[test]
    fn node_bad_params_test() {
        let args = vec!["whitelist", "127.0.0.1,localhost,192.168.0.1,"];
        let (parser, _) = prepare_parser();

        assert!(parser.parse(args).is_err());
    }
}
