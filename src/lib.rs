#[macro_use]
extern crate async_trait;

#[cfg(test)]
pub(crate) mod test_util;

pub mod configuration;

#[cfg(test)]
mod tests {
    use crate::test_util::new_test_session;

    #[tokio::test]
    async fn test_can_connect() {
        let s = new_test_session().await;
        s.query("SELECT * FROM system.local", ()).await.unwrap();
    }
}
