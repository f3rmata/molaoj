mod grpc_service;
use grpc_service::MolaDispatcher;
use grpc_service::judgedispatcher::judge_dispatcher_server::JudgeDispatcherServer;

use tonic::transport::Server;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "[::1]:50051".parse()?;
    let judgedispatcher = MolaDispatcher::default();

    Server::builder()
        .add_service(JudgeDispatcherServer::new(judgedispatcher))
        .serve(addr)
        .await?;

    Ok(())
}
