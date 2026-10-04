use lettre::transport::AsyncTransport;
use lettre::Message;
use tokio::sync::mpsc::Receiver as MpscReceiver;

pub(crate) async fn sender<T>(transport: T, mut rx: MpscReceiver<Message>) -> ()
where
    T: AsyncTransport + Sync,
    <T as AsyncTransport>::Error : std::fmt::Display,
{
    loop {
        let message = rx.recv().await.expect("Error while reading queue 'wrp': closed handle");
        if let Err(err) = transport.send(message).await {
            eprintln!("Error while sending mail: {}", err);
        }
    }
}
