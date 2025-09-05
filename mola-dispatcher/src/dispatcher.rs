use lapin::{
    BasicProperties, Connection, ConnectionProperties, options::BasicPublishOptions,
    options::QueueDeclareOptions, types::FieldTable, types::ReplyCode,
};
use tokio::sync::{Mutex, broadcast, mpsc};

// TODO: add boradcastlistener and store tasks, dispatch mq tasks here.

async fn mq_client() -> Result<(), anyhow::Error> {
    let amqp_url = std::env::var("AMQP_ADDR")
        .unwrap_or_else(|_| "amqp://rmq:rmq@127.0.0.1:5672/%2f".into());

    let conn = Connection::connect(amqp_url, ConnectionProperties::default()).await?;
    let task_channel = conn.create_channel().await?;

    let queue = task_channel
        .queue_declare(
            "hello",
            QueueDeclareOptions::default(),
            FieldTable::default(),
        )
        .await?;

    task_channel.close(200, "Bye").await?;
    conn.close(200, "Bye").await?;

    Ok(())
}

async fn postgres_client() -> Result<(), anyhow::Error> {
    unimplemented!();
}

pub async fn dispatcher() -> Result<(), anyhow::Error> {
    unimplemented!();
}
