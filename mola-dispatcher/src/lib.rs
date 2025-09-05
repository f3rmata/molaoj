pub mod dispatcher;
pub mod grpc_service;

use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{Mutex, broadcast, mpsc};

#[derive(Debug, Clone)]
struct CompileSettings {
    code: String,
    compile_cmd: Vec<String>,
    run_cmd: Vec<String>,
    stdin: String,
    env: HashMap<String, String>,
    time_limit_ms: u32,
    memory_limit_kb: u32,
    max_output_bytes: u32,
}

#[derive(Debug, Clone, Default)]
struct CompileResult {
    exit_code: i32,
    cpu_time_ms: i64,
    memory_kb: i64,
    stdout: String,
    stderr: String,
    message: String,
}

#[derive(Debug, Clone)]
pub struct Task {
    task_id: String,
    user_id: String,
    priority: u32,
    status: grpc_service::judgedispatcher::TaskStatus,
    compile_settings: CompileSettings,
    compile_result: Option<CompileResult>,
    submitted_at: Option<prost_types::Timestamp>,
    started_at: Option<prost_types::Timestamp>,
    finished_at: Option<prost_types::Timestamp>,
}

#[derive(Debug, Clone)]
pub struct TaskEntry {
    // broadcast sender 用于向多个订阅者发布日志/状态；sender.clone().subscribe() 用于创建 Receiver
    notifier: broadcast::Sender<grpc_service::judgedispatcher::TaskEvent>,
    task: Task,
}

#[derive(Debug, Clone)]
pub struct AppState {
    // 将任务 id 映射到 TaskEntry
    pub tasks: Arc<Mutex<HashMap<String, TaskEntry>>>,
    pub queue_tx: mpsc::Sender<String>,
}
