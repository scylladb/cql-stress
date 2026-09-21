use anyhow::{Context, Result};
use scylla::client::{Compression, PoolSize};
use scylla::routing::ShardAwarePortRange;

use crate::settings::{
    param::{
        types::{
            ConnectionsPerHost, ConnectionsPerShard, FlagNumericOrBool, ShardAwarePortRangeParam,
        },
        ParamsParser, SimpleParamHandle,
    },
    ParsePayload,
};

#[derive(PartialEq, Eq, Debug)]
pub struct Credentials {
    pub username: String,
    pub password: String,
}

pub struct ModeOption {
    pub compression: Option<Compression>,
    pub user_credentials: Option<Credentials>,
    pub pool_size: PoolSize,
    pub shard_aware_port_range: Option<ShardAwarePortRange>,
    pub tcp_reuse_address: Option<bool>,
}

impl ModeOption {
    pub const CLI_STRING: &'static str = "-mode";

    pub fn description() -> &'static str {
        "CQL connection options"
    }

    pub fn parse(cl_args: &mut ParsePayload) -> Result<Self> {
        let params = cl_args.remove(Self::CLI_STRING).unwrap_or_default();
        let (parser, handles) = prepare_parser();
        parser
            .parse(params)
            .context("Failed to parse -mode option parameters")?;
        Self::from_handles(handles)
    }

    pub fn print_help() {
        let (parser, _) = prepare_parser();
        parser.print_help();
    }

    pub fn print_settings(&self) {
        println!("Mode:");
        println!("  Compression: {:?}", self.compression);
        if let Some(creds) = &self.user_credentials {
            println!("  Username: {}", creds.username);
            println!("  Password: {}", creds.password);
        }
        println!("  Pool size: {:?}", self.pool_size);
        if let Some(range) = &self.shard_aware_port_range {
            println!("  Shard-aware port range: {range:?}");
        }
        if let Some(reuse) = self.tcp_reuse_address {
            println!("  TCP SO_REUSEADDR: {reuse}");
        }
    }

    fn from_handles(handles: ModeParamHandles) -> Result<ModeOption> {
        let compression = handles.compression.get().unwrap();
        let user_credentials = match (handles.username.get(), handles.password.get()) {
            (Some(username), Some(password)) => Some(Credentials { username, password }),
            (None, None) => None,
            _ => {
                return Err(anyhow::anyhow!(
                    "Both username and password need to be either set or unspecified"
                ))
            }
        };
        let pool_size = match handles.connections_per_shard.get() {
            Some(per_shard) => per_shard,
            None => handles.connections_per_host.get().unwrap(),
        };
        let shard_aware_port_range = handles.shard_aware_port_range.get();
        let tcp_reuse_address = handles.tcp_reuse_address.get();

        Ok(Self {
            compression,
            user_credentials,
            pool_size,
            shard_aware_port_range,
            tcp_reuse_address,
        })
    }
}

struct ModeParamHandles {
    compression: SimpleParamHandle<Option<Compression>>,
    username: SimpleParamHandle<String>,
    password: SimpleParamHandle<String>,
    connections_per_host: SimpleParamHandle<ConnectionsPerHost>,
    connections_per_shard: SimpleParamHandle<ConnectionsPerShard>,
    shard_aware_port_range: SimpleParamHandle<ShardAwarePortRangeParam>,
    tcp_reuse_address: SimpleParamHandle<FlagNumericOrBool>,
}

fn prepare_parser() -> (ParamsParser, ModeParamHandles) {
    let mut parser = ParamsParser::new(ModeOption::CLI_STRING);

    // `cql3` and `native` parameters are ignored but we introduce them so we support
    // the cassandra-stress CLI syntax which is used in SCT.
    let cql3 = parser.simple_param::<bool>("cql3", None, "cql3 mode (dummy parameter)", false);
    let native =
        parser.simple_param::<bool>("native", None, "native mode (dummy parameter)", false);

    let compression = parser.simple_param(
        "compression=",
        Some("none"),
        "Compression algorithm used for connections",
        false,
    );
    let username = parser.simple_param("user=", None, "username", false);
    let password = parser.simple_param("password=", None, "password", false);
    let connections_per_shard = parser.simple_param(
        "connectionsPerShard=",
        Some("1"),
        "Number of connections per shard",
        false,
    );
    let connections_per_host = parser.simple_param(
        "connectionsPerHost=",
        None,
        "Number of connections per host",
        false,
    );
    // Both knobs below lift the per-client-IP ceiling on shard-aware connections.
    // The driver binds each shard-aware connection to a local port from a fixed range
    // (49152..65535 by default) without SO_REUSEADDR, which caps one client IP at
    // 16384 such connections across all nodes. The range only matters for a per-shard
    // pool (the driver picks shard-aware ports only for `PoolSize::PerShard`), so it is
    // absent from the `connectionsPerHost` group; SO_REUSEADDR is set on every
    // connection socket and stays available in both.
    let shard_aware_port_range = parser.simple_param(
        "shardAwarePortRange=",
        None,
        "Inclusive local port range for shard-aware connections, e.g. 1024..65535 (default: 49152..65535; connectionsPerShard only)",
        false,
    );
    let tcp_reuse_address = parser.simple_param(
        "tcpReuseAddress=",
        None,
        "Set SO_REUSEADDR on connection sockets (true/false), so a local port can be reused towards each node",
        false,
    );

    // $ ./cql-stress-cassandra-stress help -mode
    // Usage: -mode cql3 native [compression=?] [user=?] [password=?] [connectionsPerShard=?] [shardAwarePortRange=?] [tcpReuseAddress=?]
    //  OR
    // Usage: -mode cql3 native [compression=?] [user=?] [password=?] [connectionsPerHost=?] [tcpReuseAddress=?]
    parser.group(&[
        &cql3,
        &native,
        &compression,
        &username,
        &password,
        &connections_per_shard,
        &shard_aware_port_range,
        &tcp_reuse_address,
    ]);
    parser.group(&[
        &cql3,
        &native,
        &compression,
        &username,
        &password,
        &connections_per_host,
        &tcp_reuse_address,
    ]);

    (
        parser,
        ModeParamHandles {
            compression,
            username,
            password,
            connections_per_host,
            connections_per_shard,
            shard_aware_port_range,
            tcp_reuse_address,
        },
    )
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;

    use crate::settings::option::{
        mode::{prepare_parser, Credentials},
        ModeOption,
    };
    use scylla::client::{Compression, PoolSize};
    use scylla::routing::ShardAwarePortRange;

    #[test]
    fn mode_good_params_test() {
        let args = vec![
            "cql3",
            "native",
            "compression=snappy",
            "user=cassandra",
            "password=cassandra",
        ];
        let (parser, handles) = prepare_parser();

        assert!(parser.parse(args).is_ok());

        let params = ModeOption::from_handles(handles).unwrap();
        assert_eq!(Some(Compression::Snappy), params.compression);
        assert_eq!(
            Some(Credentials {
                username: String::from("cassandra"),
                password: String::from("cassandra")
            }),
            params.user_credentials
        );
        // PoolSize doesn't derive Eq/PartialEq.
        match params.pool_size {
            PoolSize::PerShard(v) if v == NonZeroUsize::new(1).unwrap() => (),
            _ => panic!("Expected PoolSize::PerShard(1)"),
        }
        assert!(params.shard_aware_port_range.is_none());
        assert!(params.tcp_reuse_address.is_none());
    }

    #[test]
    fn mode_good_params_test_with_port_range_and_reuse_address() {
        let args = vec![
            "connectionsPerShard=1200",
            "shardAwarePortRange=1024..65535",
            "tcpReuseAddress=true",
        ];
        let (parser, handles) = prepare_parser();

        assert!(parser.parse(args).is_ok());

        let params = ModeOption::from_handles(handles).unwrap();
        assert_eq!(Some(true), params.tcp_reuse_address);
        // ShardAwarePortRange doesn't derive Eq/PartialEq outside the driver's own tests,
        // so compare Debug output against a value built the same way.
        assert_eq!(
            format!("{:?}", ShardAwarePortRange::new(1024..=65535).unwrap()),
            format!("{:?}", params.shard_aware_port_range.unwrap())
        );
    }

    #[test]
    fn mode_bad_params_port_range_and_reuse_address_test() {
        let bad_args = [
            // below the driver's 1024 floor
            vec!["shardAwarePortRange=100..200"],
            // empty range
            vec!["shardAwarePortRange=65535..1024"],
            // wrong separator
            vec!["shardAwarePortRange=1024-65535"],
            // out of u16
            vec!["shardAwarePortRange=1024..70000"],
            // takes a value: true/false/1/0
            vec!["tcpReuseAddress"],
            vec!["tcpReuseAddress=yes"],
            // a per-host pool never binds a shard-aware port, so the range is meaningless there
            vec!["connectionsPerHost=3", "shardAwarePortRange=1024..65535"],
        ];
        for args in bad_args {
            let (parser, _handles) = prepare_parser();
            assert!(
                parser.parse(args.clone()).is_err(),
                "expected {args:?} to be rejected"
            );
        }
    }

    #[test]
    fn mode_reuse_address_accepts_bool_and_numeric_values() {
        for (value, expected) in [("true", true), ("1", true), ("false", false), ("0", false)] {
            let arg = format!("tcpReuseAddress={value}");
            let (parser, handles) = prepare_parser();
            assert!(parser.parse(vec![arg.as_str()]).is_ok());
            assert_eq!(
                Some(expected),
                ModeOption::from_handles(handles).unwrap().tcp_reuse_address
            );
        }
    }

    #[test]
    fn mode_good_params_test_with_connections_per_host_and_reuse_address() {
        let args = vec!["connectionsPerHost=3", "tcpReuseAddress=true"];
        let (parser, handles) = prepare_parser();

        assert!(parser.parse(args).is_ok());

        let params = ModeOption::from_handles(handles).unwrap();
        assert_eq!(Some(true), params.tcp_reuse_address);
        assert!(params.shard_aware_port_range.is_none());
    }

    #[test]
    fn mode_good_params_test_with_connections_per_host() {
        let args = vec!["connectionsperhost=3"];
        let (parser, handles) = prepare_parser();

        assert!(parser.parse(args).is_ok());

        let params = ModeOption::from_handles(handles).unwrap();
        assert_eq!(None, params.compression);
        assert_eq!(None, params.user_credentials);
        // PoolSize doesn't derive Eq/PartialEq.
        match params.pool_size {
            PoolSize::PerHost(v) if v == NonZeroUsize::new(3).unwrap() => (),
            _ => panic!("Expected PoolSize::PerHost(3)"),
        }
    }

    #[test]
    fn mode_bad_params_grouping_test() {
        let args = vec!["connectionsperhost=3", "connectionspershard=1"];
        let (parser, _handles) = prepare_parser();

        assert!(parser.parse(args).is_err());
    }

    #[test]
    fn mode_bad_params_credentials_test() {
        // user is set but password is not specified
        let args = vec!["user=cassandra"];
        let (parser, handles) = prepare_parser();

        assert!(parser.parse(args).is_ok());
        assert!(ModeOption::from_handles(handles).is_err());
    }
}
