//! Asking a node directly which CQL protocol extensions it advertises.
//!
//! Leader-aware routing for strongly consistent tables rides on the
//! `TABLETS_ROUTING_V2_EXPERIMENTAL` extension: only a V2 payload carries a leader-ordered
//! replica list. The driver negotiates it automatically and exposes nothing about the
//! outcome - not the negotiated [`ProtocolFeatures`], and (since the keyspace consistency
//! mode became crate-private) no derived signal either.
//!
//! The two capabilities are genuinely independent: ScyllaDB 2026.2.x accepts
//! `consistency = 'global'` while advertising only `TABLETS_ROUTING_V1`. A run there looks
//! strongly consistent from every angle the driver shows and is not leader-routed at all,
//! which is the one failure this whole feature exists to make impossible. So the node is
//! asked itself, with the one request that needs no session, no keyspace and no auth: a bare
//! `OPTIONS`, answered by `SUPPORTED`. Parsing the reply is left to the driver's own
//! [`ProtocolFeatures`], so the extension keys stay defined in exactly one place.

use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream, ToSocketAddrs};
use std::time::Duration;

use anyhow::{Context, Result};
use scylla::frame::protocol_features::ProtocolFeatures;

/// The port a node speaks CQL on when its `-node` entry does not say.
const DEFAULT_CQL_PORT: u16 = 9042;
/// CQL binary protocol v4 - the `SUPPORTED` reply does not depend on the version, and v4 is
/// understood by every server that could possibly have tablets.
const PROTOCOL_VERSION: u8 = 0x04;
const OPTIONS_OPCODE: u8 = 0x05;
const SUPPORTED_OPCODE: u8 = 0x06;
/// A `SUPPORTED` body is a few hundred bytes; this only bounds a malformed length field.
const MAX_BODY_LEN: usize = 256 * 1024;
const PROBE_TIMEOUT: Duration = Duration::from_secs(5);

/// Returns the protocol features `node` advertises.
///
/// `node` is a `-node` entry: `host`, `host:port` or an address literal.
///
/// This speaks plaintext CQL, so it cannot probe a TLS-only node; callers that configured TLS
/// are expected not to call it. Being a startup-only, single-request probe, it runs on a
/// blocking thread rather than pulling more of tokio into the build.
pub async fn fetch_protocol_features(node: &str) -> Result<ProtocolFeatures> {
    let node = node.to_owned();
    tokio::task::spawn_blocking(move || fetch_protocol_features_blocking(&node))
        .await
        .context("The protocol extension probe panicked")?
}

fn fetch_protocol_features_blocking(node: &str) -> Result<ProtocolFeatures> {
    let address = resolve(node)?;

    let mut stream = TcpStream::connect_timeout(&address, PROBE_TIMEOUT)
        .with_context(|| format!("Failed to connect to {address}"))?;
    stream.set_read_timeout(Some(PROBE_TIMEOUT))?;
    stream.set_write_timeout(Some(PROBE_TIMEOUT))?;

    // [version][flags][stream id][opcode][body length] - an OPTIONS request has no body.
    let request = [PROTOCOL_VERSION, 0, 0, 0, OPTIONS_OPCODE, 0, 0, 0, 0];
    stream
        .write_all(&request)
        .with_context(|| format!("Failed to send an OPTIONS request to {address}"))?;

    let mut header = [0u8; 9];
    stream
        .read_exact(&mut header)
        .with_context(|| format!("Failed to read the reply from {address}"))?;

    let opcode = header[4];
    anyhow::ensure!(
        opcode == SUPPORTED_OPCODE,
        "{address} answered an OPTIONS request with opcode {opcode:#04x}, expected SUPPORTED"
    );

    let body_len = u32::from_be_bytes([header[5], header[6], header[7], header[8]]) as usize;
    anyhow::ensure!(
        body_len <= MAX_BODY_LEN,
        "{address} announced a {body_len} byte SUPPORTED body, which is not plausible"
    );

    let mut body = vec![0u8; body_len];
    stream
        .read_exact(&mut body)
        .with_context(|| format!("Failed to read the SUPPORTED body from {address}"))?;

    let supported = parse_string_multimap(&body)
        .with_context(|| format!("Failed to parse the SUPPORTED body from {address}"))?;

    Ok(ProtocolFeatures::parse_from_supported(&supported))
}

fn resolve(node: &str) -> Result<SocketAddr> {
    // `-node` accepts the same forms as the driver's known nodes: with a port or without.
    // A bare IPv6 literal has colons of its own, so only a form the resolver accepts as
    // `host:port` is treated as one.
    let candidates = node
        .to_socket_addrs()
        .or_else(|_| (node, DEFAULT_CQL_PORT).to_socket_addrs())
        .with_context(|| format!("Failed to resolve node address '{node}'"))?;

    candidates
        .into_iter()
        .next()
        .with_context(|| format!("Node address '{node}' resolved to nothing"))
}

/// Reads a CQL `[string multimap]`: a `[short]` count of `[string]` keys, each followed by a
/// `[string list]`.
fn parse_string_multimap(body: &[u8]) -> Result<HashMap<String, Vec<String>>> {
    let mut reader = Reader { body, offset: 0 };

    let entries = reader.read_short()?;
    let mut map = HashMap::with_capacity(entries as usize);
    for _ in 0..entries {
        let key = reader.read_string()?;
        let values = reader.read_short()?;
        let mut list = Vec::with_capacity(values as usize);
        for _ in 0..values {
            list.push(reader.read_string()?);
        }
        map.insert(key, list);
    }

    Ok(map)
}

struct Reader<'a> {
    body: &'a [u8],
    offset: usize,
}

impl Reader<'_> {
    fn take(&mut self, count: usize) -> Result<&[u8]> {
        let end = self
            .offset
            .checked_add(count)
            .context("Length overflow in the SUPPORTED body")?;
        let slice = self
            .body
            .get(self.offset..end)
            .context("The SUPPORTED body ended in the middle of a value")?;
        self.offset = end;
        Ok(slice)
    }

    fn read_short(&mut self) -> Result<u16> {
        let bytes = self.take(2)?;
        Ok(u16::from_be_bytes([bytes[0], bytes[1]]))
    }

    fn read_string(&mut self) -> Result<String> {
        let len = self.read_short()? as usize;
        let bytes = self.take(len)?;
        String::from_utf8(bytes.to_vec()).context("A SUPPORTED string is not valid UTF-8")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Builds a `[string multimap]` body the way a server would.
    fn multimap_body(entries: &[(&str, &[&str])]) -> Vec<u8> {
        let mut body = Vec::new();
        let push_string = |body: &mut Vec<u8>, value: &str| {
            body.extend_from_slice(&(value.len() as u16).to_be_bytes());
            body.extend_from_slice(value.as_bytes());
        };

        body.extend_from_slice(&(entries.len() as u16).to_be_bytes());
        for (key, values) in entries {
            push_string(&mut body, key);
            body.extend_from_slice(&(values.len() as u16).to_be_bytes());
            for value in *values {
                push_string(&mut body, value);
            }
        }
        body
    }

    #[test]
    fn parses_a_supported_body() {
        let body = multimap_body(&[
            ("CQL_VERSION", &["3.3.1"]),
            ("COMPRESSION", &["lz4", "snappy"]),
            ("TABLETS_ROUTING_V2_EXPERIMENTAL", &[]),
        ]);

        let supported = parse_string_multimap(&body).unwrap();

        assert_eq!(supported["CQL_VERSION"], vec!["3.3.1"]);
        assert_eq!(supported["COMPRESSION"], vec!["lz4", "snappy"]);
        assert!(supported.contains_key("TABLETS_ROUTING_V2_EXPERIMENTAL"));
    }

    /// The point of the whole module: telling a V2 node from a V1-only one. The extension
    /// key itself is the driver's business, so this goes through the driver's parser.
    #[test]
    fn tells_tablets_routing_v2_from_v1() {
        let v2 = multimap_body(&[
            ("TABLETS_ROUTING_V1", &[]),
            ("TABLETS_ROUTING_V2_EXPERIMENTAL", &[]),
        ]);
        let v1_only = multimap_body(&[("TABLETS_ROUTING_V1", &[])]);

        let parse = |body: &[u8]| {
            ProtocolFeatures::parse_from_supported(&parse_string_multimap(body).unwrap())
        };

        assert!(parse(&v2).tablets_v2_supported);
        assert!(!parse(&v1_only).tablets_v2_supported);
    }

    #[test]
    fn rejects_a_truncated_body() {
        let mut body = multimap_body(&[("CQL_VERSION", &["3.3.1"])]);
        body.truncate(body.len() - 2);

        assert!(parse_string_multimap(&body).is_err());
    }
}
