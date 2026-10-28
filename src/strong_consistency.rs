//! Strongly consistent (Raft-per-tablet) keyspaces: what every cql-stress frontend needs to
//! check before it runs against one.

mod protocol_extensions;

use anyhow::{Context, Result};
use scylla::client::session::Session;
use scylla::cluster::metadata::ConsistencyMode;

pub use protocol_extensions::fetch_protocol_features;

/// Returns the driver's view of `keyspace`'s consistency mode; `None` when the keyspace does
/// not exist at all.
///
/// The driver reports [`ConsistencyMode::Global`] only when it *both* negotiated the
/// `TABLETS_ROUTING_V2` protocol extension *and* read `consistency = 'global'` for the
/// keyspace, so one value proves both halves of leader-aware routing.
pub async fn keyspace_consistency_mode(
    session: &Session,
    keyspace: &str,
) -> Result<Option<ConsistencyMode>> {
    // DDL issued just before may have raced the background metadata refresh, so force one
    // before reading the mode back: this is the same snapshot the driver's own routing
    // decisions are made from.
    session
        .refresh_metadata()
        .await
        .context("Failed to refresh cluster metadata")?;

    Ok(session
        .get_cluster_state()
        .get_keyspace(keyspace)
        .map(|ks| ks.consistency_mode.clone()))
}

/// The start-up failure for a keyspace that was meant to be strongly consistent and is not.
///
/// It carries [`STRONG_CONSISTENCY_UNAVAILABLE_CODE`] and, unless `tls` is set (the probe
/// speaks plaintext CQL), each of `nodes`' answer on `TABLETS_ROUTING_V2_EXPERIMENTAL`.
pub async fn unavailable_error(
    keyspace: &str,
    reported_mode: &str,
    ddl: &str,
    nodes: &[String],
    tls: bool,
) -> anyhow::Error {
    let diagnosis = diagnose_v2(nodes, tls).await;
    anyhow::anyhow!(strong_consistency_failure_message(
        keyspace,
        reported_mode,
        ddl,
        &diagnosis,
    ))
}

/// Explains, as far as it can be established from outside, why the driver does not see
/// the keyspace as strongly consistent.
///
/// A mode other than `Global` has several independent causes and the value itself cannot
/// tell them apart. Asking the node whether it advertises `TABLETS_ROUTING_V2_EXPERIMENTAL`
/// separates the most confusing one - a server that cannot do leader routing at all, yet
/// happily stores `consistency = 'global'` - from an ordinary "this keyspace was not
/// created strongly consistent".
///
/// Every configured contact node is asked, not just the first: the session is built from
/// the whole node list, so generalising from one of them gets the answer exactly
/// backwards on a cluster midway through a rolling enable, where the first contact point
/// carries the flag and another does not. When the nodes disagree, that disagreement *is*
/// the diagnosis and is reported as such.
///
/// Returns a sentence to append to the failure, or an empty string when there is nothing
/// useful to add. It runs only on the failure path, so a healthy run pays nothing for it.
async fn diagnose_v2(nodes: &[String], tls: bool) -> String {
    if nodes.is_empty() {
        return String::new();
    }

    // The probe speaks plaintext CQL and cannot reach a TLS-only node.
    if tls {
        return String::from(
            "\nThe configured nodes were not asked whether they advertise \
             TABLETS_ROUTING_V2_EXPERIMENTAL: the probe speaks plaintext CQL and this \
             run uses TLS.",
        );
    }

    // A long -node list would make the failure unreadable, and the answer is a cluster
    // property: a handful of nodes is enough to tell a uniform cluster from a mixed one.
    const MAX_PROBED_NODES: usize = 8;
    let probed = &nodes[..nodes.len().min(MAX_PROBED_NODES)];

    let outcomes = futures::future::join_all(
        probed
            .iter()
            .map(|node| async move { (node.as_str(), fetch_protocol_features(node).await) }),
    )
    .await;

    let mut with_v2 = Vec::new();
    let mut without_v2 = Vec::new();
    let mut unreachable = Vec::new();
    for (node, result) in &outcomes {
        match result {
            Ok(features) if features.tablets_v2_supported => with_v2.push(*node),
            Ok(_) => without_v2.push(*node),
            Err(error) => unreachable.push(format!("{node} ({error:#})")),
        }
    }

    summarise_v2_probe(
        &with_v2,
        &without_v2,
        &unreachable,
        probed.len(),
        nodes.len(),
    )
}

/// Marks the startup failure raised when a run asked for `consistency=global` and would not
/// have measured it.
///
/// The integration tests have to tell this apart from a binary that is simply broken - a
/// panic, a renamed CLI option, an unreachable node - because they *skip* on the first and
/// must *fail* on the second. Matching on prose would make every reword a silent un-skip, so
/// the failure carries a stable code and `tools/test_cs_strong_consistency.py` matches on it.
pub const STRONG_CONSISTENCY_UNAVAILABLE_CODE: &str = "STRONG_CONSISTENCY_UNAVAILABLE";

/// Builds the failure raised when `consistency=global` was requested but the driver does not
/// see the keyspace as strongly consistent.
///
/// Split out from [`unavailable_error`] so a unit test can pin the diagnostic code
/// without a live cluster - see [`STRONG_CONSISTENCY_UNAVAILABLE_CODE`].
fn strong_consistency_failure_message(
    keyspace: &str,
    reported_mode: &str,
    ddl: &str,
    diagnosis: &str,
) -> String {
    format!(
        "Requested consistency=global, but the driver does not see keyspace '{keyspace}' as \
         strongly consistent (mode: {reported_mode}). This run would not measure strong \
         consistency. The driver reports Global only once it has both negotiated \
         TABLETS_ROUTING_V2 and read consistency='global' for the keyspace, so any of these \
         breaks it:\n\
         - the server does not run with \
         --experimental-features=strongly-consistent-tables;\n\
         - the cluster feature gating strongly consistent tables is not enabled yet - it \
         turns on only once every node carries that flag, so a partially upgraded cluster \
         lands here;\n\
         - the server does not advertise TABLETS_ROUTING_V2_EXPERIMENTAL, which is a \
         capability separate from accepting consistency='global' (ScyllaDB 2026.2.x has the \
         second without the first);\n\
         - keyspace '{keyspace}' already exists as an eventually consistent keyspace (CREATE \
         KEYSPACE IF NOT EXISTS will not upgrade it - drop it first);\n\
         - the keyspace is not tablet-based (non-tablet keyspaces reject the consistency \
         option; SimpleStrategy may not get tablets).\n\
         DDL used: {ddl}{diagnosis}\n\
         (diagnostic code: {STRONG_CONSISTENCY_UNAVAILABLE_CODE})"
    )
}

/// Turns the per-node `TABLETS_ROUTING_V2_EXPERIMENTAL` answers into the sentence appended to
/// a failed strong-consistency check.
///
/// Split out from the probe itself so the wording - which is the whole point of the
/// diagnostic - can be tested without a server. A conclusion is drawn only when the nodes
/// agree; when they disagree, the disagreement is the diagnosis, because a cluster part-way
/// through enabling the experimental feature is the most confusing state this check can land
/// in and the one a single-node probe reports exactly backwards.
fn summarise_v2_probe(
    with_v2: &[&str],
    without_v2: &[&str],
    unreachable: &[String],
    probed: usize,
    total: usize,
) -> String {
    let mut report = String::from(
        "\nAsked the configured nodes whether they advertise TABLETS_ROUTING_V2_EXPERIMENTAL",
    );
    if total > probed {
        report.push_str(&format!(
            " (first {probed} of {total}, {} not probed)",
            total - probed
        ));
    }
    report.push_str(":\n");

    match (with_v2.is_empty(), without_v2.is_empty()) {
        // Nobody advertises it: these servers cannot route to leaders at all.
        (true, false) => report.push_str(&format!(
            "- none of them do ({}), so no node can hand the driver a leader-ordered replica \
             list. This is a capability separate from accepting consistency='global': \
             ScyllaDB 2026.2.x stores 'global' in system_schema.scylla_keyspaces while \
             advertising only TABLETS_ROUTING_V1, which is exactly why the mode above reads \
             as eventual.",
            without_v2.join(", ")
        )),
        // All of them do: the extension is not the missing half.
        (false, true) => report.push_str(&format!(
            "- all of them do ({}), so these servers can route to tablet leaders - the \
             keyspace itself is what is not strongly consistent.",
            with_v2.join(", ")
        )),
        // Mixed.
        (false, false) => report.push_str(&format!(
            "- some do ({}) and some do not ({}). The cluster is part-way through enabling \
             --experimental-features=strongly-consistent-tables: the cluster feature that \
             gates consistency='global' stays off until every node carries the flag, so the \
             keyspace cannot be created strongly consistent yet even though some nodes could \
             already route to leaders. Finish the rollout and retry.",
            with_v2.join(", "),
            without_v2.join(", ")
        )),
        // Nothing answered at all.
        (true, true) => report.push_str("- none of them could be reached."),
    }

    if !unreachable.is_empty() {
        report.push_str(&format!(
            "\n- could not be asked: {}",
            unreachable.join(", ")
        ));
    }

    report
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The diagnostic's wording is the whole point of it, and it is the message a user reads
    /// when they are already confused. A single-node probe reports the mixed case exactly
    /// backwards, so that row in particular is worth pinning.
    #[test]
    fn summarise_v2_probe_test() {
        let all = summarise_v2_probe(&["a", "b"], &[], &[], 2, 2);
        assert!(all.contains("all of them do (a, b)"), "{all}");
        assert!(
            all.contains("the keyspace itself is what is not strongly consistent"),
            "{all}"
        );

        let none = summarise_v2_probe(&[], &["a", "b"], &[], 2, 2);
        assert!(none.contains("none of them do (a, b)"), "{none}");
        assert!(none.contains("TABLETS_ROUTING_V1"), "{none}");

        // The case a first-node-only probe gets backwards: it would have reported either
        // "this server can route to leaders" or "no node can", depending on the list order.
        let mixed = summarise_v2_probe(&["a"], &["b"], &[], 2, 2);
        assert!(mixed.contains("some do (a) and some do not (b)"), "{mixed}");
        assert!(mixed.contains("part-way through enabling"), "{mixed}");

        let unreachable = summarise_v2_probe(&[], &[], &[String::from("a (refused)")], 1, 1);
        assert!(
            unreachable.contains("none of them could be reached"),
            "{unreachable}"
        );
        assert!(unreachable.contains("a (refused)"), "{unreachable}");

        // One unreachable node must not discard the answers the others gave.
        let partial = summarise_v2_probe(&["a"], &[], &[String::from("b (timeout)")], 2, 2);
        assert!(partial.contains("all of them do (a)"), "{partial}");
        assert!(
            partial.contains("could not be asked: b (timeout)"),
            "{partial}"
        );

        // A long -node list is capped, and the message says so rather than implying the
        // unprobed nodes were found to agree.
        let capped = summarise_v2_probe(&["a"], &[], &[], 1, 20);
        assert!(capped.contains("first 1 of 20, 19 not probed"), "{capped}");
    }

    /// `tools/test_cs_strong_consistency.py` tells "this server cannot do leader-aware routing"
    /// (skip the suite) from "the binary is broken" (fail the job) by matching this code. Pin it
    /// here so rewording the failure breaks a fast unit test rather than silently turning the
    /// integration job green by skipping everything.
    #[test]
    fn strong_consistency_failure_carries_its_diagnostic_code_test() {
        let message = strong_consistency_failure_message(
            "keyspace1",
            "Eventual",
            "CREATE KEYSPACE ...",
            "\ndiagnosis here",
        );

        assert!(
            message.contains(STRONG_CONSISTENCY_UNAVAILABLE_CODE),
            "the integration probe matches on this code: {message}"
        );
        assert!(message.contains("keyspace1"), "{message}");
        assert!(message.contains("diagnosis here"), "{message}");
    }

    /// The two answers `diagnose_v2` gives without asking any node: nothing to say without
    /// nodes, and a TLS run is told why its nodes were not asked rather than probed in
    /// plaintext.
    #[tokio::test]
    async fn diagnose_v2_without_probing_test() {
        assert_eq!(diagnose_v2(&[], false).await, "");

        let tls = diagnose_v2(&[String::from("127.0.0.1")], true).await;
        assert!(tls.contains("the probe speaks plaintext CQL"), "{tls}");
        assert!(tls.contains("this run uses TLS"), "{tls}");
    }
}
