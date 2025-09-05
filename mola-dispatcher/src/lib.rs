pub mod grpc_service;

use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct Task {
    task_id: String,
    user_id: String,
    priority: u32,
    status: grpc_service::judgedispatcher::TaskStatus,
    code: String,
    compile_cmd: Vec<String>,
    run_cmd: Vec<String>,
    stdin: String,
    env: HashMap<String, String>,
    time_limit_ms: u32,
    memory_limit_kb: u32,
    max_output_bytes: u32,
    submitted_at: Option<prost_types::Timestamp>,
    started_at: Option<prost_types::Timestamp>,
    finished_at: Option<prost_types::Timestamp>,
}

impl Task {}
