use std::pin::Pin;
use std::sync::Arc;
use tokio_stream::{Stream, wrappers::ReceiverStream};
use tonic::{Request, Response, Status};

pub mod judgedispatcher {
    tonic::include_proto!("task.v1");
}

use judgedispatcher::judge_dispatcher_server::JudgeDispatcher;
use judgedispatcher::{
    CancelTaskReply, CancelTaskRequest, GetTaskReply, GetTaskRequest, ListTasksReply,
    ListTasksRequest, SubmitTaskReply, SubmitTaskRequest, TaskEvent, TaskEventsRequest,
};

#[derive(Debug, Default)]
pub struct MolaDispatcher {
    task_events: Arc<Vec<TaskEvent>>,
}

#[tonic::async_trait]
impl JudgeDispatcher for MolaDispatcher {
    async fn submit_task(
        &self,
        request: Request<SubmitTaskRequest>,
    ) -> Result<Response<SubmitTaskReply>, Status> {
        unimplemented!()
    }

    type SubmitTaskAndWaitStream = ReceiverStream<Result<TaskEvent, Status>>;
    async fn submit_task_and_wait(
        &self,
        request: Request<SubmitTaskRequest>,
    ) -> Result<Response<Self::SubmitTaskAndWaitStream>, Status> {
        unimplemented!()
    }

    async fn get_task(
        &self,
        request: Request<GetTaskRequest>,
    ) -> Result<Response<GetTaskReply>, Status> {
        unimplemented!()
    }

    async fn cancel_task(
        &self,
        request: Request<CancelTaskRequest>,
    ) -> Result<Response<CancelTaskReply>, Status> {
        unimplemented!()
    }

    async fn list_tasks(
        &self,
        request: Request<ListTasksRequest>,
    ) -> Result<Response<ListTasksReply>, Status> {
        unimplemented!()
    }

    type TaskEventsStream = Pin<Box<dyn Stream<Item = Result<TaskEvent, Status>> + Send + 'static>>;
    async fn task_events(
        &self,
        request: Request<tonic::Streaming<TaskEventsRequest>>,
    ) -> Result<Response<Self::TaskEventsStream>, Status> {
        unimplemented!()
    }
}
