use anyhow::{Context, Result};
use scylla::client::{Compression, PoolSize};

use crate::settings::{
    param::{
        types::{ConnectionsPerHost, ConnectionsPerShard},
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

        Ok(Self {
            compression,
            user_credentials,
            pool_size,
        })
    }
}

struct ModeParamHandles {
    compression: SimpleParamHandle<Option<Compression>>,
    username: SimpleParamHandle<String>,
    password: SimpleParamHandle<String>,
    connections_per_host: SimpleParamHandle<ConnectionsPerHost>,
    connections_per_shard: SimpleParamHandle<ConnectionsPerShard>,
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

    // $ ./cql-stress-cassandra-stress help -node
    // Usage: -mode cql3 native [compression=?] [user=?] [password=?] [connectionsPerShard=?]
    //  OR
    // Usage: -mode cql3 native [compression=?] [user=?] [password=?] [connectionsPerHost=?]
    parser.group(&[
        &cql3,
        &native,
        &compression,
        &username,
        &password,
        &connections_per_shard,
    ]);
    parser.group(&[
        &cql3,
        &native,
        &compression,
        &username,
        &password,
        &connections_per_host,
    ]);

    (
        parser,
        ModeParamHandles {
            compression,
            username,
            password,
            connections_per_host,
            connections_per_shard,
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
