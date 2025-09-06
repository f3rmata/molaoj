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
use sandbox_builder::{nsjail_builder, sandbox_run};
use std::time::Duration;

mod sandbox_builder;

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
                match handle_delivery(&data).await {
                    Ok(()) => {
                        delivery.ack(BasicAckOptions::default()).await?;
                    }
                    Err(e) => {
                        error!("task handle error: {e:#}");
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

async fn handle_delivery(payload: &[u8]) -> anyhow::Result<()> {
    let (task, envs, work_dir, compile_cmd, run_cmd) = nsjail_builder(payload).await?;

    // 执行编译（可选）
    if let Some(first) = compile_cmd.first() {
        if !first.is_empty() {
            info!("compiling...");
            let (status, stdout, stderr) = sandbox_run(
                &compile_cmd,
                &envs,
                &work_dir,
                None,
                Duration::from_millis(task.settings.time_limit_ms as u64),
                task.settings.max_output_bytes as usize,
            )
            .await?;
            info!(
                "\n[compile] exit={} stdout(len={}):\n{}\n[compile] stderr(len={}):\n{}",
                status.code().unwrap_or(-1),
                stdout.len(),
                stdout,
                stderr.len(),
                stderr
            );
            if !status.success() {
                // 编译失败即结束
                return Ok(());
            }
        } else {
            info!("no compile_cmd given, skipping compiling...");
        }
    }

    // 运行
    if let Some(first) = run_cmd.first() {
        if first.is_empty() {
            info!("running...");
            let (status, stdout, stderr) = sandbox_run(
                &run_cmd,
                &envs,
                &work_dir,
                Some(task.settings.stdin.as_bytes()),
                Duration::from_millis(task.settings.time_limit_ms as u64),
                task.settings.max_output_bytes as usize,
            )
            .await?;
            info!(
                "\n[run] exit={} stdout(len={}):\n{}\n[run] stderr(len={}):\n{}",
                status.code().unwrap_or(-1),
                stdout.len(),
                stdout,
                stderr.len(),
                stderr
            );
        } else {
            info!("no run_cmd given, skipping running...");
        }
    }

    Ok(())
}
