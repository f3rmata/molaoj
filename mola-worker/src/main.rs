use futures_lite::stream::StreamExt;
use lapin::{Connection, ConnectionProperties, types::FieldTable};

mod sandbox_builder;

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    let amqp_url = "amqp://guest:guest@localhost:5672/%2F";
    let conn = Connection::connect(amqp_url, ConnectionProperties::default()).await?;

    // 创建消息通道
    let channel_a = conn.create_channel().await?;

    // 接收消息
    let mut consumer = channel_a
        .basic_consume(
            "hello",
            "my_consumer",
            lapin::options::BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await?;

    while let Some(delivery) = consumer.next().await {
        match delivery {
            Ok(_) => {}
            Err(e) => {
                eprintln!("{}", e)
            }
        }
    }

    channel_a.close(200, "Bye").await?;
    conn.close(200, "Bye").await?;

    Ok(())
}
