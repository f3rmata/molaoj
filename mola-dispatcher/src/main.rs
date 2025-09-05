use mola_dispatcher::AppState;
use mola_dispatcher::dispatcher::{persist_task_to_postgres, publish_task_to_rabbitmq};
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

    // 启动 Postgresql 和 Rabbitmq 客户端
    let state_clone = state.clone();
    let dispatcher_handle = tokio::spawn(async move {
        println!("starting dispatcher...");
        let mut rx = rx;
        while let Some(task_id) = rx.recv().await {
            if let Err(e) = persist_task_to_postgres(&state_clone, &task_id).await {
                eprintln!("persist to PG failed: {}", e);
            }
            if let Err(e) = publish_task_to_rabbitmq(&state_clone, &task_id).await {
                eprintln!("publish to MQ failed: {}", e);
            }
        }
    });

    // 启动 gRPC 服务端
    let judgedispatcher = GRPCService {
        state: state.clone(),
    };

    let addr = std::env::var("GRPC_SERVER_ADDR")
        .unwrap_or_else(|_| "[::1]:50051".into())
        .parse()?;

    println!("starting grpc server...");
    Server::builder()
        .add_service(JudgeDispatcherServer::new(judgedispatcher))
        .serve_with_shutdown(addr, async {
            let _ = tokio::signal::ctrl_c().await;
            eprintln!("grpc stopping...");
        })
        .await?;

    drop(tx);
    drop(state);

    let _ = dispatcher_handle.abort();
    println!("dispatcher stopping...");

    Ok(())
}
