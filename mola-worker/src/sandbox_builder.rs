use std::{collections::HashMap, path::PathBuf, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    process::Command,
    time::timeout,
};

fn nsjail_builder() -> Result<(), anyhow::Error> {
    Ok(())
}

// TODO: add sandbox feature
pub async fn sandbox_run(
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
