use mola_dispatcher::AppState;
use mola_dispatcher::grpc_service::GRPCService;
use mola_dispatcher::grpc_service::judgedispatcher::judge_dispatcher_server::JudgeDispatcherServer;

use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{Mutex, mpsc};
use tonic::transport::Server;

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    let (tx, rx) = mpsc::channel::<String>(128);
    let state = AppState {
        tasks: Arc::new(Mutex::new(HashMap::new())),
        queue_tx: tx.clone(),
    };

    let addr = "[::1]:50051".parse()?;
    let judgedispatcher = GRPCService { state };

    Server::builder()
        .add_service(JudgeDispatcherServer::new(judgedispatcher))
        .serve(addr)
        .await?;

    Ok(())
}
