use prost_types::Timestamp;
use std::pin::Pin;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio_stream::{Stream, wrappers::ReceiverStream};
use tonic::{Request, Response, Status};
use uuid::Uuid;

pub mod judgedispatcher {
    tonic::include_proto!("task.v1");
}

use judgedispatcher::judge_dispatcher_server::JudgeDispatcher;
use judgedispatcher::{
    CancelTaskReply, CancelTaskRequest, GetTaskReply, GetTaskRequest, ListTasksReply,
    ListTasksRequest, SubmitTaskReply, SubmitTaskRequest, TaskEvent, TaskEventsRequest, TaskStatus,
};

#[derive(Debug, Default)]
pub struct MolaDispatcher {}

fn get_timestamp(st: SystemTime) -> Timestamp {
    let dur = st.duration_since(UNIX_EPOCH).unwrap();
    Timestamp {
        seconds: dur.as_secs() as i64,
        nanos: dur.subsec_nanos() as i32,
    }
}

#[tonic::async_trait]
impl JudgeDispatcher for MolaDispatcher {
    async fn submit_task(
        &self,
        _request: Request<SubmitTaskRequest>,
    ) -> Result<Response<SubmitTaskReply>, Status> {
        let task_id = Uuid::new_v4().to_string();

        let reply = SubmitTaskReply {
            task_id,
            status: TaskStatus::Queued.into(),
            submitted_at: Some(get_timestamp(SystemTime::now())),
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
