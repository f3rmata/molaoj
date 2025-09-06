use log::{debug, info};
use serde::Deserialize;
use std::{collections::HashMap, path::PathBuf, time::Duration};
use tokio::{
    fs,
    io::{AsyncReadExt, AsyncWriteExt},
    process::Command,
    time::timeout,
};

#[derive(Debug, Deserialize)]
pub struct TaskMsg {
    pub task_id: String,
    pub settings: Settings,
}

#[derive(Debug, Deserialize)]
pub struct Settings {
    code: String,
    compile_cmd: Vec<String>,
    run_cmd: Vec<String>,
    pub stdin: String,
    env: HashMap<String, String>,
    pub time_limit_ms: u32,
    pub memory_limit_kb: u32,
    pub max_output_bytes: u32,
}

fn normalize_cmd(cmd: &[String]) -> Vec<String> {
    cmd.iter()
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
        .collect()
}

async fn write_code_file(path: &PathBuf, content: &str) -> anyhow::Result<()> {
    let mut f = fs::File::create(path).await?;
    f.write_all(content.as_bytes()).await?;
    Ok(())
}

// TODO: add nsjail wrpper cmd
pub async fn nsjail_builder(
    payload: &[u8],
) -> Result<
    (
        TaskMsg,
        HashMap<String, String>,
        PathBuf,
        Vec<String>,
        Vec<String>,
    ),
    anyhow::Error,
> {
    let task: TaskMsg = serde_json::from_slice(payload)?;
    info!("received task: {}", task.task_id);

    let work_dir = std::env::temp_dir().join(format!("molaworker-{}", &task.task_id));

    fs::create_dir_all(&work_dir).await?;

    // 将代码写入文件；不知道语言时，用通用后缀
    let code_file = work_dir.join("code.src");
    let code_path_vec: Vec<String> = vec![code_file.to_string_lossy().into()];
    write_code_file(&code_file, &task.settings.code).await?;

    debug!("work_dir: {}", work_dir.to_str().unwrap_or_default());
    debug!("code_dir: {}", code_file.to_str().unwrap_or_default());

    // 统一的环境变量 + 用户自定义 env
    let mut envs = task.settings.env.clone();
    envs.insert("TASK_CODE_PATH".into(), code_file.to_string_lossy().into());
    envs.insert("TASK_WORK_DIR".into(), work_dir.to_string_lossy().into());

    let base_compile_cmd = normalize_cmd(&task.settings.compile_cmd);
    let base_run_cmd = normalize_cmd(&task.settings.run_cmd);

    let mut compile_cmd = Vec::new();
    compile_cmd.extend(base_compile_cmd.iter().cloned());
    compile_cmd.extend(code_path_vec.iter().cloned());

    info!("compile: {:?}", compile_cmd);

    let mut run_cmd = Vec::new();
    run_cmd.extend(base_run_cmd.iter().cloned());
    run_cmd.extend(code_path_vec.iter().cloned());

    info!("run: {:?}", run_cmd);

    Ok((task, envs, work_dir, compile_cmd, run_cmd))
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
