use futures_lite::stream::StreamExt;
use lapin::{
    Connection, ConnectionProperties,
    options::{
        BasicAckOptions, BasicConsumeOptions, BasicNackOptions, BasicQosOptions,
        QueueDeclareOptions,
    },
    types::FieldTable,
};
use serde::Deserialize;
use std::{collections::HashMap, path::PathBuf, time::Duration};
use tokio::{
    fs,
    io::{AsyncReadExt, AsyncWriteExt},
    process::Command,
    time::timeout,
};

mod sandbox_builder;

#[derive(Debug, Deserialize)]
struct TaskMsg {
    task_id: String,
    user_id: String,
    priority: u32,
    status: String,
    // 这些时间字段目前用不上，但 JSON 会带着
    submitted_at: Option<String>,
    started_at: Option<String>,
    finished_at: Option<String>,
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

    println!("mola-worker started. Waiting for tasks on '{}'", queue_name);

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
                        eprintln!("task handle error: {e:#}");
                        // 根据需要选择是否重回队列；这里保守重试
                        delivery
                            .nack(BasicNackOptions {
                                requeue: true,
                                multiple: false,
                            })
                            .await?;
                    }
                }
                // 小提示：你也可以根据错误类型决定是否 requeue
            }
            Err(e) => {
                eprintln!("consumer error: {e}");
            }
        }
    }

    channel.close(200, "Bye").await.ok();
    conn.close(200, "Bye").await.ok();
    Ok(())
}

async fn handle_delivery(payload: &[u8]) -> anyhow::Result<()> {
    let task: TaskMsg = serde_json::from_slice(payload)?;
    println!("received task: {}", task.task_id);

    let work_dir = std::env::temp_dir().join(format!("molaworker-{}", &task.task_id));
    fs::create_dir_all(&work_dir).await?;

    // 将代码写入文件；不知道语言时，用通用后缀
    let code_file = work_dir.join("Main.src");
    write_code_file(&code_file, &task.settings.code).await?;

    // 统一的环境变量 + 用户自定义 env
    let mut envs = task.settings.env.clone();
    envs.insert("TASK_CODE_PATH".into(), code_file.to_string_lossy().into());
    envs.insert("TASK_WORK_DIR".into(), work_dir.to_string_lossy().into());

    // 执行编译（可选）
    if !task.settings.compile_cmd.is_empty() {
        let compile_cmd = substitute_args(&task.settings.compile_cmd, &code_file, &work_dir);
        println!("compile: {:?}", compile_cmd);
        let (status, stdout, stderr) = run_command_with_limits(
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
    }

    // 运行
    if !task.settings.run_cmd.is_empty() {
        let run_cmd = substitute_args(&task.settings.run_cmd, &code_file, &work_dir);
        println!("run: {:?}", run_cmd);
        let (status, stdout, stderr) = run_command_with_limits(
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
    }

    Ok(())
}

fn substitute_args(args: &[String], code_file: &PathBuf, work_dir: &PathBuf) -> Vec<String> {
    let code_file_s = code_file.to_string_lossy();
    let work_dir_s = work_dir.to_string_lossy();
    args.iter()
        .map(|s| {
            s.replace("{code_file}", &code_file_s)
                .replace("{work_dir}", &work_dir_s)
        })
        .collect()
}

async fn write_code_file(path: &PathBuf, content: &str) -> anyhow::Result<()> {
    let mut f = fs::File::create(path).await?;
    f.write_all(content.as_bytes()).await?;
    Ok(())
}

async fn run_command_with_limits(
    argv: &[String],
    envs: &HashMap<String, String>,
    work_dir: &PathBuf,
    stdin_data: Option<&[u8]>,
    limit: Duration,
    max_output: usize,
) -> anyhow::Result<(std::process::ExitStatus, String, String)> {
    if argv.is_empty() {
        anyhow::bail!("empty argv");
    }
    let mut cmd = Command::new(&argv[0]);
    if argv.len() > 1 {
        cmd.args(&argv[1..]);
    }
    cmd.current_dir(work_dir);
    cmd.envs(envs);
    cmd.kill_on_drop(true);
    if stdin_data.is_some() {
        cmd.stdin(std::process::Stdio::piped());
    }
    cmd.stdout(std::process::Stdio::piped());
    cmd.stderr(std::process::Stdio::piped());

    let mut child = cmd.spawn()?;

    // 写入 stdin
    if let Some(data) = stdin_data {
        if let Some(mut stdin) = child.stdin.take() {
            let data = data.to_vec();
            tokio::spawn(async move {
                let _ = stdin.write_all(&data).await;
            });
        }
    }

    // 并发读取 stdout / stderr（带大小限制）
    let mut child_stdout = child.stdout.take();
    let mut child_stderr = child.stderr.take();
    let max_out = max_output;

    let stdout_task = tokio::spawn(async move {
        let mut buf = Vec::new();
        if let Some(mut reader) = child_stdout.take() {
            let mut chunk = [0u8; 8192];
            loop {
                let n = reader.read(&mut chunk).await?;
                if n == 0 {
                    break;
                }
                let remaining = max_out.saturating_sub(buf.len());
                if remaining > 0 {
                    let to_copy = remaining.min(n);
                    buf.extend_from_slice(&chunk[..to_copy]);
                }
            }
        }
        Ok::<Vec<u8>, std::io::Error>(buf)
    });

    let stderr_task = tokio::spawn(async move {
        let mut buf = Vec::new();
        if let Some(mut reader) = child_stderr.take() {
            let mut chunk = [0u8; 8192];
            loop {
                let n = reader.read(&mut chunk).await?;
                if n == 0 {
                    break;
                }
                let remaining = max_out.saturating_sub(buf.len());
                if remaining > 0 {
                    let to_copy = remaining.min(n);
                    buf.extend_from_slice(&chunk[..to_copy]);
                }
            }
        }
        Ok::<Vec<u8>, std::io::Error>(buf)
    });

    // 等待进程结束（带超时），保留 child 以便超时后 kill
    let status = match timeout(limit, child.wait()).await {
        Ok(res) => res?,
        Err(_) => {
            // 超时则杀死进程并回收
            let _ = child.kill().await;
            let _ = child.wait().await;
            anyhow::bail!("process timed out");
        }
    };

    // 收集输出
    let stdout_bytes = match stdout_task.await {
        Ok(Ok(v)) => v,
        _ => Vec::new(),
    };
    let stderr_bytes = match stderr_task.await {
        Ok(Ok(v)) => v,
        _ => Vec::new(),
    };

    let mut stdout = String::from_utf8_lossy(&stdout_bytes).into_owned();
    let mut stderr = String::from_utf8_lossy(&stderr_bytes).into_owned();

    if stdout.len() > max_output {
        stdout.truncate(max_output);
    }
    if stderr.len() > max_output {
        stderr.truncate(max_output);
    }

    Ok((status, stdout, stderr))
}
