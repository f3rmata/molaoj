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
use sandbox_builder::sandbox_run;
use serde::Deserialize;
use std::{collections::HashMap, path::PathBuf, time::Duration};
use tokio::{fs, io::AsyncWriteExt};

mod sandbox_builder;

#[derive(Debug, Deserialize)]
struct TaskMsg {
    task_id: String,
    settings: Settings,
}

#[derive(Debug, Deserialize)]
struct Settings {
    code: String,
    compile_cmd: Vec<String>,
    run_cmd: Vec<String>,
    stdin: String,
    env: HashMap<String, String>,
    time_limit_ms: u32,
    memory_limit_kb: u32,
    max_output_bytes: u32,
}

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
    let task: TaskMsg = serde_json::from_slice(payload)?;
    println!("{:?}", task);
    info!("received task: {}", task.task_id);

    let work_dir = std::env::temp_dir().join(format!("molaworker-{}", &task.task_id));

    fs::create_dir_all(&work_dir).await?;

    // 将代码写入文件；不知道语言时，用通用后缀
    let code_file = work_dir.join("code.src");
    let code_path_vec: Vec<String> = vec![code_file.to_string_lossy().into()];
    write_code_file(&code_file, &task.settings.code).await?;

    println!("work_dir: {}", work_dir.to_str().unwrap_or_default());
    println!("code_dir: {}", code_file.to_str().unwrap_or_default());

    // 统一的环境变量 + 用户自定义 env
    let mut envs = task.settings.env.clone();
    envs.insert("TASK_CODE_PATH".into(), code_file.to_string_lossy().into());
    envs.insert("TASK_WORK_DIR".into(), work_dir.to_string_lossy().into());

    let base_compile_cmd = normalize_cmd(&task.settings.compile_cmd);
    let base_run_cmd = normalize_cmd(&task.settings.run_cmd);

    let mut compile_cmd = Vec::new();
    compile_cmd.extend(task.settings.compile_cmd.iter().cloned());
    compile_cmd.extend(code_path_vec.iter().cloned());

    println!("compile: {:?}", compile_cmd);

    let mut run_cmd = Vec::new();
    run_cmd.extend(task.settings.run_cmd.iter().cloned());
    run_cmd.extend(code_path_vec.iter().cloned());

    println!("run: {:?}", run_cmd);

    // 执行编译（可选）
    if !base_compile_cmd.is_empty() {
        println!("compiling...");
        let (status, stdout, stderr) = sandbox_run(
            &compile_cmd,
            &envs,
            &work_dir,
            None,
            Duration::from_millis(task.settings.time_limit_ms as u64),
            task.settings.max_output_bytes as usize,
        )
        .await?;
        println!(
            "[compile] exit={} stdout(len={}):\n{}\n[compile] stderr(len={}):\n{}",
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

    // 运行
    if !base_run_cmd.is_empty() {
        println!("running...");
        let (status, stdout, stderr) = sandbox_run(
            &run_cmd,
            &envs,
            &work_dir,
            Some(task.settings.stdin.as_bytes()),
            Duration::from_millis(task.settings.time_limit_ms as u64),
            task.settings.max_output_bytes as usize,
        )
        .await?;
        println!(
            "[run] exit={} stdout(len={}):\n{}\n[run] stderr(len={}):\n{}",
            status.code().unwrap_or(-1),
            stdout.len(),
            stdout,
            stderr.len(),
            stderr
        );
    } else {
        info!("no run_cmd given, skipping running...");
    }

    Ok(())
}

async fn write_code_file(path: &PathBuf, content: &str) -> anyhow::Result<()> {
    let mut f = fs::File::create(path).await?;
    f.write_all(content.as_bytes()).await?;
    Ok(())
}

fn normalize_cmd(cmd: &[String]) -> Vec<String> {
    cmd.iter()
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
        .collect()
}
