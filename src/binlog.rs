use std::collections::HashMap;

use tokio::sync::mpsc::{Sender as MpscSender};

use herald::Uuid;

use mysql_binlog_connector_rust::{
    binlog_stream::BinlogStream,
    event::event_data::EventData,
    column::column_value::ColumnValue,
};

pub(crate) async fn listener(mut stream: BinlogStream, tx: MpscSender<(Uuid, u64)>) -> () {
    let mut tables: HashMap<u64, String> = HashMap::new();
    loop {
        let (_, data) = match stream.read().await {
            Ok(v) => v,
            Err(err) => {
                eprintln!("Error while reading binlog stream: {}", err);
                continue;
            }
        };
        match data {
            EventData::TableMap(e) => {
                tables.insert(e.table_id, format!("{}.{}", e.database_name, e.table_name));
            },
            EventData::WriteRows(e) => {
                if let Some(name) = tables.get(&e.table_id) {
                    if name == "herald.registration_queue" {
                        for row in e.rows.iter() {
                            let Some(ColumnValue::String(registration_id)) = row.column_values.first() else {
                                eprintln!("Error while parsing registration queue: malformed registration_id");
                                continue;
                            };
                            let Some(ColumnValue::Bit(status)) = row.column_values.last() else {
                                eprintln!("Error while parsing registration queue: malformed status");
                                continue;
                            };
                            let registration_id = Uuid::from_bytes(*registration_id.as_array().unwrap());
                            let status = *status;
                            tx.send((registration_id, status)).await.expect("Error while writing queue 'upd': closed handle");
                        }
                    }
                }
            },
            _ => {},
        }
    }
}
