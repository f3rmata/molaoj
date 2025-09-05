use prost_types::Timestamp;
use std::pin::Pin;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::broadcast;
use tokio_stream::{Stream, wrappers::ReceiverStream};
use tonic::{Request, Response, Status};
use uuid::Uuid;

use crate::{AppState, CompileSettings, Task, TaskEntry};

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

        let compile_settings = CompileSettings {
            code: req_body.code,
            compile_cmd: req_body.compile_cmd,
            run_cmd: req_body.run_cmd,
            stdin: req_body.stdin,
            env: req_body.env,
            time_limit_ms: req_body.time_limit_ms,
            memory_limit_kb: req_body.memory_limit_kb,
            max_output_bytes: req_body.max_output_bytes,
        };

        let task_req = Task {
            submitted_at,
            compile_settings,
            status: TaskStatus::Queued,
            compile_result: None,
            task_id: task_id.clone(),
            user_id: req_body.user_id,
            priority: req_body.priority,
            started_at: None,
            finished_at: None,
        };

        // 创建通知器：容量 128 的广播通道
        let (tx, _rx) = broadcast::channel(128);
        let entry = TaskEntry {
            notifier: tx.clone(),
            task: task_req,
        };

        let mut map = self.state.tasks.lock().await;
        map.insert(task_id.clone(), entry);

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
        request: Request<GetTaskRequest>,
    ) -> Result<Response<GetTaskReply>, Status> {
        let req_body = request.into_inner();
        let task_id = req_body.task_id;

        // 取出并克隆条目，避免长时间持有 tasks 锁
        let entry = {
            let map = self.state.tasks.lock().await;
            map.get(&task_id).cloned()
        }
        .ok_or_else(|| Status::not_found(format!("task {} not found", task_id)))?;

        let t = entry.task;
        let compile_result = t.compile_result.unwrap_or_default();

        let reply = GetTaskReply {
            task_id: t.task_id,
            status: t.status.into(),
            submitted_at: t.submitted_at,
            started_at: t.started_at,
            finished_at: t.finished_at,
            exit_code: compile_result.exit_code,
            cpu_time_ms: compile_result.cpu_time_ms,
            memory_kb: compile_result.memory_kb,
            stdout: compile_result.stdout,
            stderr: compile_result.stderr,
            message: compile_result.message,
            metadata: Default::default(),
        };

        Ok(Response::new(reply))
    }

    // TODO: need test!
    async fn cancel_task(
        &self,
        request: Request<CancelTaskRequest>,
    ) -> Result<Response<CancelTaskReply>, Status> {
        let req_body = request.into_inner();

        let task_id = req_body.task_id.clone();
        let user_id = req_body.user_id;

        // 取出并克隆条目，避免长时间持有 tasks 锁
        let mut map = self.state.tasks.lock().await;
        let entry = match map.get_mut(&task_id) {
            Some(e) => e,
            None => return Err(Status::not_found(format!("task {} not found", task_id))),
        };

        // 简单的权限校验：必须为提交者本人
        if !entry.task.user_id.is_empty() && entry.task.user_id != user_id {
            return Err(Status::permission_denied("not allowed to cancel this task"));
        }

        // 锁定状态并判断可否取消
        let st = &mut entry.task.status;
        match *st {
            TaskStatus::Queued | TaskStatus::Running => {
                // 状态转为 CANCELLED，并标记完成时间
                *st = TaskStatus::Cancelled;
                entry.task.finished_at = Some(get_timestamp(SystemTime::now()));

                // 复制必要对象，释放 map 锁后再广播
                let tx = entry.notifier.clone();

                // 广播取消事件（忽略没有订阅者的错误）
                let _ = tx.send(TaskEvent {
                    task_id: task_id.clone(),
                    payload: Some(judgedispatcher::task_event::Payload::Status(
                        TaskStatus::Cancelled.into(),
                    )),
                    time: Some(get_timestamp(SystemTime::now())),
                });

                Ok(Response::new(CancelTaskReply {
                    task_id,
                    cancelled: true,
                    reason: String::from("User cancelled."),
                }))
            }
            _ => {
                // 已经结束或已取消
                Ok(Response::new(CancelTaskReply {
                    task_id,
                    cancelled: false,
                    reason: "task already finished".to_string(),
                }))
            }
        }
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
