use log::info;
use std::time::Duration;
use tokio::fs;
use tokio::process::Command;

use crate::sandbox_builder::{nsjail_builder, sandbox_run};

pub async fn handle_delivery(payload: &[u8]) -> anyhow::Result<()> {
    let (task, envs, work_dir, compile_cmd, run_cmd) = nsjail_builder(payload).await?;

    let mut command = Command::new("cd");
    command.arg(format!("{}", work_dir.to_string_lossy()));
    info!(
        "changing working directory to {}",
        work_dir.to_string_lossy()
    );

    // 执行编译（可选）
    if let Some(first) = task.settings.compile_cmd.first() {
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
    if let Some(first) = task.settings.run_cmd.first() {
        if !first.is_empty() {
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

    // 清理工作目录
    fs::remove_dir_all(work_dir).await?;

    Ok(())
}
