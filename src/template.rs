use std::collections::HashMap;

use handlebars::{Handlebars, DirectorySourceOptionsBuilder};
use tokio::sync::mpsc::{Sender as MpscSender, Receiver as MpscReceiver};

use herald::template;
use herald::types::contract::Contract;
use herald::sqlx::MySqlConnection;
use herald::Uuid;

pub(crate) async fn renderer(mut conn: MySqlConnection, mut rx: MpscReceiver<(Uuid, u64)>, tx: MpscSender<String>) -> () {
    let mut cache: HashMap<u64, Option<String>> = HashMap::new();
    let mut handlebars = Handlebars::new();
    handlebars.register_templates_directory("skel/", DirectorySourceOptionsBuilder::default().tpl_extension(".hbs").build().unwrap()).expect("Error while reading template directory");
    handlebars.set_strict_mode(true);
    handlebars.register_escape_fn(handlebars::no_escape);
    loop {
        let (registration_id, status) = rx.recv().await.expect("Error while reading queue 'upd': closed handle");
        if !cache.contains_key(&status) {
            let slug = template::query(&mut conn, status).await.expect("Error while fetching template slug");
            cache.insert(status, slug);
        }
        let slug = cache.get(&status).expect("Error while reading template slug cache: cache empty");
        if let Some(slug) = slug {
            let contract = Contract::fetch(slug.as_str(), &mut conn, registration_id).await.expect("Error while fetching contract data");
            let message = handlebars.render(slug, &contract.into_inner()).expect("Error while rendering mail template"); // TODO: move into separate thread via spawn-blocking
            tx.send(message).await.expect("Error while writing queue 'rnd': closed handle");
        }
        //if let Err(err) = sqlx::query_file!("sql/registration_queue/delete.sql", registration_id, status).execute(&mut conn).await {
        //    eprintln!("Error while clearing registration queue: {}", err);
        //}
    }
}
