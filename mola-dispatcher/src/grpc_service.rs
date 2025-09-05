use prost_types::Timestamp;
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::{Mutex, broadcast, mpsc};
use tokio_stream::{Stream, wrappers::ReceiverStream};
use tonic::{Request, Response, Status};
use uuid::Uuid;

use crate::Task;

pub mod judgedispatcher {
    tonic::include_proto!("task.v1");
}

use judgedispatcher::judge_dispatcher_server::JudgeDispatcher;
use judgedispatcher::{
    CancelTaskReply, CancelTaskRequest, GetTaskReply, GetTaskRequest, ListTasksReply,
    ListTasksRequest, SubmitTaskReply, SubmitTaskRequest, TaskEvent, TaskEventsRequest, TaskStatus,
};

fn get_timestamp(st: SystemTime) -> Timestamp {
    let dur = st.duration_since(UNIX_EPOCH).unwrap();
    Timestamp {
        seconds: dur.as_secs() as i64,
        nanos: dur.subsec_nanos() as i32,
    }
}

#[derive(Debug, Clone)]
pub struct TaskEntry {
    // broadcast sender 用于向多个订阅者发布日志/状态；sender.clone().subscribe() 用于创建 Receiver
    notifier: broadcast::Sender<TaskEvent>,
    status: Arc<Mutex<TaskStatus>>,
    task: Task,
}

#[derive(Debug, Clone)]
pub struct AppState {
    // 将任务 id 映射到 TaskEntry
    pub tasks: Arc<Mutex<HashMap<String, TaskEntry>>>,
    pub queue_tx: mpsc::Sender<String>,
}

#[derive(Debug)]
pub struct GRPCService {
    pub state: AppState,
}

#[tonic::async_trait]
impl JudgeDispatcher for GRPCService {
    async fn submit_task(
        &self,
        request: Request<SubmitTaskRequest>,
    ) -> Result<Response<SubmitTaskReply>, Status> {
        let task_id = Uuid::new_v4().to_string();
        let req_body = request.into_inner();
        let submitted_at = Some(get_timestamp(SystemTime::now()));

        let task_req = Task {
            submitted_at,
            task_id: task_id.clone(),
            user_id: req_body.user_id,
            priority: req_body.priority,
            code: req_body.code,
            compile_cmd: req_body.compile_cmd,
            run_cmd: req_body.run_cmd,
            stdin: req_body.stdin,
            env: req_body.env,
            time_limit_ms: req_body.time_limit_ms,
            memory_limit_kb: req_body.memory_limit_kb,
            max_output_bytes: req_body.max_output_bytes,
            status: TaskStatus::Queued,
            started_at: None,
            finished_at: None,
        };

        // 创建通知器：容量 128 的广播通道
        let (tx, _rx) = broadcast::channel(128);
        let entry = TaskEntry {
            notifier: tx.clone(),
            status: Arc::new(Mutex::new(TaskStatus::Queued)),
            task: task_req,
        };

        let _ = self
            .state
            .queue_tx
            .send(task_id.clone())
            .await
            .map_err(|e| Status::internal(format!("queue send err: {}", e)))?;

        let reply = SubmitTaskReply {
            task_id,
            submitted_at,
            status: TaskStatus::Queued.into(),
        };
        Ok(Response::new(reply))
    }

    type SubmitTaskAndWaitStream = ReceiverStream<Result<TaskEvent, Status>>;
    async fn submit_task_and_wait(
        &self,
        _request: Request<SubmitTaskRequest>,
    ) -> Result<Response<Self::SubmitTaskAndWaitStream>, Status> {
        unimplemented!()
    }

    async fn get_task(
        &self,
        _request: Request<GetTaskRequest>,
    ) -> Result<Response<GetTaskReply>, Status> {
        unimplemented!()
    }

    async fn cancel_task(
        &self,
        _request: Request<CancelTaskRequest>,
    ) -> Result<Response<CancelTaskReply>, Status> {
        unimplemented!()
    }

    async fn list_tasks(
        &self,
        _request: Request<ListTasksRequest>,
    ) -> Result<Response<ListTasksReply>, Status> {
        unimplemented!()
    }

    type TaskEventsStream = Pin<Box<dyn Stream<Item = Result<TaskEvent, Status>> + Send + 'static>>;
    async fn task_events(
        &self,
        _request: Request<tonic::Streaming<TaskEventsRequest>>,
    ) -> Result<Response<Self::TaskEventsStream>, Status> {
        unimplemented!()
    }
}
