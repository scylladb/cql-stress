use std::sync::Arc;

use scylla::{Session, SessionBuilder};

pub fn get_contact_node_uri() -> String {
    std::env::var("SCYLLA_URI").unwrap_or_else(|_| "127.0.0.1:9042".to_string())
}

pub async fn new_test_session() -> Arc<Session> {
    let sess = SessionBuilder::new()
        .known_node(get_contact_node_uri())
        .build()
        .await
        .unwrap();

    Arc::new(sess)
}
