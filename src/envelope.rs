use lettre::Message;
use lettre::message::header::{HeaderName, HeaderValue};
use tokio::sync::mpsc::{Sender as MpscSender, Receiver as MpscReceiver};

pub(crate) async fn wrapper(mut rx: MpscReceiver<String>, tx: MpscSender<Message>) -> () {
    loop {
        let message = rx.recv().await.expect("Error while reading queue 'rnd': closed handle");
        let mut builder = Message::builder();
        let (headers, body) = message.split_once("\n\n").expect("Malformed template");
        for header in headers.split("\n") {
            let (name, value) = header.split_once(": ").expect("Malformed header in template");
            let name = HeaderName::new_from_ascii(String::from(name)).expect("Malformed header name in template");
            builder = builder.raw_header(HeaderValue::new(name, String::from(value)));
        }
        let email = builder.body(String::from(body)).expect("Unexpected error while attaching mail body");
        tx.send(email).await.expect("Error while writing queue 'wrp': closed handle");
    }
}

#[cfg(test)]
mod tests {
    use std::assert_matches;
    use lettre::Message;
    use tokio::sync::mpsc;

    #[tokio::test]
    async fn message_envelope_parser_succeeds() {
        let (rnd_tx, rnd_rx) = mpsc::channel::<String>(1);
        let (wrp_tx, mut wrp_rx) = mpsc::channel::<Message>(1);
        let render = "From: Debug <debug@example.com>\nTo: User <user@example.com>\nSubject: Your Debugging Envelope\n\nGreetings,\nwe are glad to announce your test is successful.\n\nBest regards,\nUnittest Team";
        tokio::spawn(super::wrapper(rnd_rx, wrp_tx));
        rnd_tx.send(render.to_owned()).await.unwrap();
        let message = wrp_rx.recv().await;
        assert_matches!(message, Some(_))
    }
}
