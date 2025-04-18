use crate::settings::param::types::{FlagNumericOrBool, NonEmptyString, NotSupported};
use crate::settings::param::{ParamsParser, SimpleParamHandle};
use anyhow::Result;
use std::collections::HashMap;

#[derive(Debug, Clone, Default)]
pub struct TransportOption {
    pub factory: Option<NotSupported>,
    pub truststore: Option<String>,
    pub truststore_password: Option<NotSupported>,
    pub keystore: Option<String>,
    pub keystore_password: Option<NotSupported>,
    pub ssl_protocol: Option<NotSupported>,
    pub ssl_alg: Option<NotSupported>,
    pub store_type: Option<NotSupported>,
    pub ssl_ciphers: Option<NotSupported>,
    pub hostname_verification: Option<bool>,
}

struct TransportParamHandles {
    factory: SimpleParamHandle<NotSupported>,
    truststore: SimpleParamHandle<NonEmptyString>,
    truststore_password: SimpleParamHandle<NotSupported>,
    keystore: SimpleParamHandle<NonEmptyString>,
    keystore_password: SimpleParamHandle<NotSupported>,
    ssl_protocol: SimpleParamHandle<NotSupported>,
    ssl_alg: SimpleParamHandle<NotSupported>,
    store_type: SimpleParamHandle<NotSupported>,
    ssl_ciphers: SimpleParamHandle<NotSupported>,
    hostname_verification: SimpleParamHandle<FlagNumericOrBool>,
}

fn prepare_parser() -> (ParamsParser, TransportParamHandles) {
    let mut parser = ParamsParser::new(TransportOption::CLI_STRING);
    let factory = parser.simple_param("factory=", None, "SSL factory class (unsupported)", false);
    let truststore = parser.simple_param("truststore=", None, "Path to truststore file", false);
    let truststore_password = parser.simple_param(
        "truststore-password=",
        None,
        "Truststore password (unsupported)",
        false,
    );
    let keystore = parser.simple_param("keystore=", None, "Path to keystore file", false);
    let keystore_password = parser.simple_param(
        "keystore-password=",
        None,
        "Keystore password (unsupported)",
        false,
    );
    let ssl_protocol =
        parser.simple_param("ssl-protocol=", None, "SSL protocol (unsupported)", false);
    let ssl_alg = parser.simple_param("ssl-alg=", None, "SSL algorithm (unsupported)", false);
    let store_type = parser.simple_param("store-type=", None, "Store type (unsupported)", false);
    let ssl_ciphers = parser.simple_param("ssl-ciphers=", None, "SSL ciphers (unsupported)", false);
    let hostname_verification = parser.simple_param(
        "hostname-verification=",
        None,
        "Enable hostname verification (true/false/1/0)",
        false,
    );
    parser.group(&[
        &factory,
        &truststore,
        &truststore_password,
        &keystore,
        &keystore_password,
        &ssl_protocol,
        &ssl_alg,
        &store_type,
        &ssl_ciphers,
        &hostname_verification,
    ]);
    (
        parser,
        TransportParamHandles {
            factory,
            truststore,
            truststore_password,
            keystore,
            keystore_password,
            ssl_protocol,
            ssl_alg,
            store_type,
            ssl_ciphers,
            hostname_verification,
        },
    )
}

impl TransportOption {
    pub const CLI_STRING: &'static str = "-transport";

    pub fn parse(payload: &mut HashMap<String, Vec<&str>>) -> Result<Self> {
        let params = payload.remove(Self::CLI_STRING).unwrap_or_default();
        let (parser, handles) = prepare_parser();
        parser.parse(params)?;
        TransportOption::from_handles(handles)
    }

    fn from_handles(handles: TransportParamHandles) -> Result<TransportOption> {
        let factory = handles.factory.get();
        let truststore = handles.truststore.get();
        let truststore_password = handles.truststore_password.get();
        let keystore = handles.keystore.get();
        let keystore_password = handles.keystore_password.get();
        let ssl_protocol = handles.ssl_protocol.get();
        let ssl_alg = handles.ssl_alg.get();
        let store_type = handles.store_type.get();
        let ssl_ciphers = handles.ssl_ciphers.get();
        let hostname_verification = handles.hostname_verification.get();

        Ok(TransportOption {
            factory,
            truststore,
            truststore_password,
            keystore,
            keystore_password,
            ssl_protocol,
            ssl_alg,
            store_type,
            ssl_ciphers,
            hostname_verification,
        })
    }

    pub fn print_settings(&self) {
        println!("Transport settings:");
        if self.factory.is_some() {
            println!("  factory: (unsupported)");
        }
        if let Some(ref v) = self.truststore {
            println!("  truststore: {}", v);
        }
        if self.truststore_password.is_some() {
            println!("  truststore-password: (unsupported)");
        }
        if let Some(ref v) = self.keystore {
            println!("  keystore: {}", v);
        }
        if self.keystore_password.is_some() {
            println!("  keystore-password: (unsupported)");
        }
        if self.ssl_protocol.is_some() {
            println!("  ssl-protocol: (unsupported)");
        }
        if self.ssl_alg.is_some() {
            println!("  ssl-alg: (unsupported)");
        }
        if self.store_type.is_some() {
            println!("  store-type: (unsupported)");
        }
        if self.ssl_ciphers.is_some() {
            println!("  ssl-ciphers: (unsupported)");
        }
        if let Some(v) = self.hostname_verification {
            println!("  hostname-verification: {}", v);
        }
    }

    pub fn description() -> &'static str {
        "transport and SSL options"
    }

    pub fn print_help() {
        let (parser, _) = prepare_parser();
        parser.print_help();
    }

    pub fn generate_ssl_context(&self) -> anyhow::Result<openssl::ssl::SslContext> {
        use anyhow::Context;
        use openssl::ssl::{SslContextBuilder, SslFiletype, SslMethod, SslVerifyMode};
        use std::fs;

        let mut builder = SslContextBuilder::new(SslMethod::tls())?;
        builder.set_verify(match self.hostname_verification {
            Some(true) => SslVerifyMode::PEER,
            _ => SslVerifyMode::NONE,
        });

        if let Some(ref truststore) = self.truststore {
            let ca_path = fs::canonicalize(truststore).with_context(|| {
                format!("Failed to canonicalize truststore path: {}", truststore)
            })?;
            builder
                .set_ca_file(&ca_path)
                .with_context(|| format!("Failed to set CA file: {}", ca_path.display()))?;
        }
        if let Some(ref keystore) = self.keystore {
            let key_path = fs::canonicalize(keystore)
                .with_context(|| format!("Failed to canonicalize keystore path: {}", keystore))?;
            builder
                .set_certificate_file(&key_path, SslFiletype::PEM)
                .with_context(|| {
                    format!("Failed to set certificate file: {}", key_path.display())
                })?;
            builder
                .set_private_key_file(&key_path, SslFiletype::PEM)
                .with_context(|| {
                    format!("Failed to set private key file: {}", key_path.display())
                })?;
        }
        Ok(builder.build())
    }
}
