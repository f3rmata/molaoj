use futures_lite::stream::StreamExt;
use lapin::{
    Connection, ConnectionProperties,
    options::{
        BasicAckOptions, BasicConsumeOptions, BasicNackOptions, BasicQosOptions,
        QueueDeclareOptions,
    },
    types::FieldTable,
};
use log::{error, info};

mod sandbox_builder;
mod worker;

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    env_logger::init();

    let amqp_url = std::env::var("AMQP_ADDR")
        .unwrap_or_else(|_| "amqp://guest:guest@127.0.0.1:5672/%2f".into());
    let queue_name = std::env::var("AMQP_TASK_QUEUE").unwrap_or_else(|_| "tasks.submit".into());

    let conn = Connection::connect(&amqp_url, ConnectionProperties::default()).await?;
    let channel = conn.create_channel().await?;

    // 仅保证队列存在（幂等）
    channel
        .queue_declare(
            &queue_name,
            QueueDeclareOptions {
                durable: true,
                auto_delete: false,
                exclusive: false,
                passive: false,
                nowait: false,
            },
            FieldTable::default(),
        )
        .await?;

    // 单并发消费
    channel
        .basic_qos(1, BasicQosOptions { global: false })
        .await?;

    let mut consumer = channel
        .basic_consume(
            &queue_name,
            "mola-worker",
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await?;

    info!("mola-worker started. Waiting for tasks on '{}'", queue_name);

    while let Some(delivery) = consumer.next().await {
        match delivery {
            Ok(delivery) => {
                // let tag = delivery.delivery_tag;
                let data = delivery.data.clone();
                match worker::handle_delivery(&data).await {
                    Ok(()) => {
                        delivery.ack(BasicAckOptions::default()).await?;
                    }
                    Err(e) => {
                        error!("sandbox run error: {e:#}");
                        // 根据需要选择是否重回队列；这里保守重试
                        delivery
                            .nack(BasicNackOptions {
                                requeue: false,
                                multiple: false,
                            })
                            .await?;
                    }
                }
                // TODO: 根据错误类型决定是否 requeue
            }
            Err(e) => {
                error!("consumer error: {e}");
            }
        }
    }

    channel.close(200, "Bye").await.ok();
    conn.close(200, "Bye").await.ok();
    Ok(())
}
