use std::time::Duration;
use std::env;

use lettre::transport::smtp::AsyncSmtpTransport;
use lettre::transport::smtp::authentication::Credentials;
use lettre::Tokio1Executor;
use lettre::Message;
use lettre::transport::smtp::client::{Tls, TlsParameters};
use tokio::{
    task::JoinSet,
    sync::mpsc,
};

use mysql_binlog_connector_rust::{
    binlog_client::{BinlogClient, StartPosition},
};

use herald::sqlx::{Connection, MySqlConnection};
use herald::Uuid;

mod binlog;
mod template;
mod envelope;
mod transport;

#[tokio::main]
async fn main() -> () {
    let url = env::var("DATABASE_URL").expect("DATABASE_URL parameter missing");
    let url = url.as_str();

    let (upd_tx, upd_rx) = mpsc::channel::<(Uuid, u64)>(8);
    let (rnd_tx, rnd_rx) = mpsc::channel::<String>(16);
    let (wrp_tx, wrp_rx) = mpsc::channel::<Message>(16);

    let mut upd_client = BinlogClient::new(url, 9, StartPosition::Gtid("".to_owned()))
        .with_master_heartbeat(Duration::from_secs(5))
        .with_read_timeout(Duration::from_secs(60))
        .with_keepalive(Duration::from_secs(60), Duration::from_secs(10));
    let upd_stream = upd_client.connect();
    let rnd_conn = MySqlConnection::connect(url);
    let tls = TlsParameters::builder("localhost".to_owned())
        .dangerous_accept_invalid_certs(true)
        .build().unwrap();
    let transport = AsyncSmtpTransport::<Tokio1Executor>::from_url("smtps://localhost:1025").unwrap();
    let transport = transport.tls(Tls::Wrapper(tls));
    let transport = transport.credentials(Credentials::new(String::from("user"), String::from("pass"))).build(); // TODO: Hardcoded
    let mut tasks = JoinSet::<()>::new();
    tasks.spawn(crate::binlog::listener(upd_stream.await.unwrap(), upd_tx));
    tasks.spawn(crate::template::renderer(rnd_conn.await.unwrap(), upd_rx, rnd_tx));
    tasks.spawn(crate::envelope::wrapper(rnd_rx, wrp_tx));
    tasks.spawn(crate::transport::sender(transport, wrp_rx));
    let _ = tasks.join_next().await;
    
    unreachable!()
}
/*
#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_mustache_render() {
        let (tx, mut rx) = mpsc::channel::<(String, String)>(1);
        #[derive(serde::Serialize)]
        struct Thing {
            name: String,
            topic: String,
        }
        let thing = Thing {
            name: "Testo Husbando".to_string(),
            topic: "Event 'Unittesting is great'".to_string(),
        };
        let template = Template {
            contract: "unittest".to_string(),
            body: "Hello {{{name}}},\nwe are glad to confirm your registration to {{{topic}}}.\n\nBest regards,\nUnittest Team".to_string(),
            subject: "Your registration to {{{topic}}}".to_string(),
        };
        let template = Box::pin(template);
        tokio::spawn(render(thing, template, tx));
        let (subject, body) = rx.recv().await.expect("handle closed prematurely");
        assert_eq!(subject, "Your registration to Event 'Unittesting is great'");
        assert_eq!(body, "Hello Testo Husbando,\nwe are glad to confirm your registration to Event 'Unittesting is great'.\n\nBest regards,\nUnittest Team");
    }
}
*/
