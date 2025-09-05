use lapin::{
    BasicProperties, Channel, Connection, ConnectionProperties,
    options::{BasicPublishOptions, QueueDeclareOptions},
    types::FieldTable,
};

use serde_json::{Value, json};
use tokio_postgres::{NoTls, types::Json};

use crate::{AppState, Task};

// NOTE: 将任务序列化为 JSON，用于 MQ 发送或 PG JSON 列
fn task_to_json(task: &Task) -> Value {
    // 将 TaskStatus 枚举转换成字符串（与 proto 枚举一致的大写风格）
    fn status_to_str(s: &crate::grpc_service::judgedispatcher::TaskStatus) -> &'static str {
        use crate::grpc_service::judgedispatcher::TaskStatus as S;
        match s {
            S::Unspecified => "TASK_STATUS_UNSPECIFIED",
            S::Queued => "QUEUED",
            S::Running => "RUNNING",
            S::Succeeded => "SUCCEEDED",
            S::Failed => "FAILED",
            S::Cancelled => "CANCELLED",
            S::Timeout => "TIMEOUT",
            S::RuntimeError => "RUNTIME_ERROR",
        }
    }

    let settings = json!({
        "code": task.compile_settings.code,
        "compile_cmd": task.compile_settings.compile_cmd,
        "run_cmd": task.compile_settings.run_cmd,
        "stdin": task.compile_settings.stdin,
        "env": task.compile_settings.env,
        "time_limit_ms": task.compile_settings.time_limit_ms,
        "memory_limit_kb": task.compile_settings.memory_limit_kb,
        "max_output_bytes": task.compile_settings.max_output_bytes,
    });

    json!({
        "task_id": task.task_id,
        "user_id": task.user_id,
        "priority": task.priority,
        "status": status_to_str(&task.status),
        "submitted_at": task.submitted_at.unwrap_or_default().to_string(),
        "started_at": task.started_at.unwrap_or_default().to_string(),
        "finished_at": task.finished_at.unwrap_or_default().to_string(),
        "settings": settings
    })
}

// 创建/获取 RabbitMQ Channel（一次性简单实现）
async fn mk_mq_channel(amqp_url: &str) -> Result<(Connection, Channel), anyhow::Error> {
    let conn = Connection::connect(amqp_url, ConnectionProperties::default()).await?;
    let ch = conn.create_channel().await?;
    Ok((conn, ch))
}

// 发布任务到 RabbitMQ 指定队列（任务从 AppState 读取）
pub async fn publish_task_to_rabbitmq(
    state: &AppState,
    task_id: &str,
) -> Result<(), anyhow::Error> {
    let amqp_url = std::env::var("AMQP_ADDR")
        .unwrap_or_else(|_| "amqp://guest:guest@127.0.0.1:5672/%2f".into());
    let queue_name =
        std::env::var("AMQP_TASK_QUEUE").unwrap_or_else(|_| "tasks.submit".to_string());

    // 取任务并克隆，避免长时间持锁
    let task = {
        let map = state.tasks.lock().await;
        map.get(task_id)
            .map(|e| e.task.clone())
            .ok_or_else(|| anyhow::anyhow!("task {} not found in state", task_id))?
    };

    let payload = task_to_json(&task).to_string().into_bytes();

    // 连接 MQ 并声明队列（幂等）
    let (conn, ch) = mk_mq_channel(&amqp_url).await?;
    ch.queue_declare(
        &queue_name,
        QueueDeclareOptions {
            durable: true,
            auto_delete: false,
            exclusive: false,
            passive: false,
            nowait: false,
        },
        FieldTable::default(),
    )
    .await?;

    // 发布
    let confirm = ch
        .basic_publish(
            "", // default exchange
            &queue_name,
            BasicPublishOptions::default(),
            &payload,
            BasicProperties::default()
                .with_content_type("application/json".into())
                .with_delivery_mode(2), // persistent
        )
        .await?
        .await?; // wait for broker confirmation

    // 关闭
    ch.close(200, "Bye").await.ok();
    conn.close(200, "Bye").await.ok();

    if confirm.is_nack() {
        return Err(anyhow::anyhow!("broker negatively acknowledged publish"));
    }

    Ok(())
}

// 将任务写入 PostgreSQL（幂等 upsert）
pub async fn persist_task_to_postgres(
    state: &AppState,
    task_id: &str,
) -> Result<(), anyhow::Error> {
    let pg_dsn = std::env::var("PG_DSN")
        .or_else(|_| std::env::var("DATABASE_URL"))
        .unwrap_or_else(|_| "postgres://postgres:postgres@127.0.0.1:5432/molaoj".into());

    // 取任务并克隆，避免长时间持锁
    let task = {
        let map = state.tasks.lock().await;
        map.get(task_id)
            .map(|e| e.task.clone())
            .ok_or_else(|| anyhow::anyhow!("task {} not found in state", task_id))?
    };

    // 连接 PG
    let (client, connection) = tokio_postgres::connect(&pg_dsn, NoTls).await?;
    // 在后台驱动连接
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("[PG] connection error: {}", e);
        }
    });

    // 创建表（最简结构，时间字段用 TEXT 存 RFC3339 或 prost Timestamp 的 JSON 也可）
    client
        .execute(
            r#"
            CREATE TABLE IF NOT EXISTS tasks (
                task_id      TEXT PRIMARY KEY,
                user_id      TEXT NOT NULL,
                priority     INTEGER NOT NULL,
                status       TEXT NOT NULL,
                submitted_at TEXT NULL,
                started_at   TEXT NULL,
                finished_at  TEXT NULL,
                settings     JSONB NOT NULL,
                result       JSONB
            )
            "#,
            &[],
        )
        .await?;

    let t_json = task_to_json(&task);

    // 拆解 JSON，用于列写入
    let status = t_json["status"]
        .as_str()
        .unwrap_or("TASK_STATUS_UNSPECIFIED")
        .to_string();
    let submitted_at = t_json["submitted_at"].to_string(); // 存 JSON 字符串或 "null"
    let started_at = t_json["started_at"].to_string();
    let finished_at = t_json["finished_at"].to_string();

    // 将 "null" 处理成 NULL
    let submitted_at = if submitted_at == "null" {
        None
    } else {
        Some(submitted_at)
    };
    let started_at = if started_at == "null" {
        None
    } else {
        Some(started_at)
    };
    let finished_at = if finished_at == "null" {
        None
    } else {
        Some(finished_at)
    };

    let settings = t_json["settings"].clone();
    let result = t_json["result"].clone();

    // upsert
    client
        .execute(
            r#"
            INSERT INTO tasks
                (task_id, user_id, priority, status, submitted_at, started_at, finished_at, settings, result)
            VALUES
                ($1,      $2,      $3,       $4,     $5,          $6,         $7,          $8,        $9)
            ON CONFLICT (task_id) DO UPDATE SET
                user_id = EXCLUDED.user_id,
                priority = EXCLUDED.priority,
                status = EXCLUDED.status,
                submitted_at = EXCLUDED.submitted_at,
                started_at = EXCLUDED.started_at,
                finished_at = EXCLUDED.finished_at,
                settings = EXCLUDED.settings,
                result = EXCLUDED.result
            "#,
            &[
                &task.task_id,
                &task.user_id,
                &(task.priority as i32),
                &status,
                &submitted_at,
                &started_at,
                &finished_at,
                &Json(settings),
                &Json(result),
            ],
        )
        .await?;

    Ok(())
}
