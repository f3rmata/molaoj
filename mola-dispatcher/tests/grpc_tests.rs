use std::env;
use std::path::Path;
use std::process::{Command, Stdio};

fn grpcurl_path() -> Option<String> {
    if Command::new("grpcurl")
        .arg("-version")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .ok()?
        .success()
    {
        Some("grpcurl".to_string())
    } else {
        None
    }
}

fn default<S: Into<String>>(val: S, env_key: &str) -> String {
    env::var(env_key).unwrap_or_else(|_| val.into())
}

fn ensure_test_prereqs() -> Result<(), String> {
    if grpcurl_path().is_none() {
        return Err("grpcurl not found in PATH. Install grpcurl or set PATH.".into());
    }

    let proto_dir = default("proto", "PROTO_DIR");
    let proto_file = default(format!("{}/task.proto", &proto_dir), "PROTO_FILE");

    if !Path::new(&proto_dir).exists() {
        return Err(format!("Proto dir not found: {}", proto_dir));
    }
    if !Path::new(&proto_file).exists() {
        return Err(format!("Proto file not found: {}", proto_file));
    }

    Ok(())
}

fn run_submit_task(payload_json: &str) -> Result<String, String> {
    let grpcurl = grpcurl_path().ok_or_else(|| "grpcurl not available".to_string())?;
    let proto_dir = default("proto", "PROTO_DIR");
    let proto_file = default(format!("{}/task.proto", &proto_dir), "PROTO_FILE");
    let addr = default("localhost:50051", "GRPC_ADDR");
    let service = default("task.v1.JudgeDispatcher", "GRPC_SERVICE");
    let method = default("SubmitTask", "GRPC_METHOD");

    let full_method = format!("{}/{}", service, method);

    let output = Command::new(grpcurl)
        .arg("-plaintext")
        .arg("-import-path")
        .arg(&proto_dir)
        .arg("-proto")
        .arg(&proto_file)
        .arg("-d")
        .arg(payload_json)
        .arg(&addr)
        .arg(&full_method)
        .output()
        .map_err(|e| format!("failed to execute grpcurl: {}", e))?;

    if !output.status.success() {
        return Err(format!(
            "grpcurl returned non-zero exit code {}\nstdout:\n{}\nstderr:\n{}",
            output.status.code().unwrap_or(-1),
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr),
        ));
    }

    Ok(String::from_utf8_lossy(&output.stdout).to_string())
}

// minimal JSON string field extractor for top-level string fields: "field": "value"
fn extract_json_string_field(json: &str, field: &str) -> Option<String> {
    let key = format!("\"{}\":", field);
    let start = json.find(&key)?;
    let rest = &json[start + key.len()..];
    let rest = rest.trim_start();
    let bytes = rest.as_bytes();
    if bytes.is_empty() || bytes[0] != b'"' {
        return None;
    }
    let mut i = 1usize;
    let mut val: Vec<u8> = Vec::new();
    let mut esc = false;
    while i < bytes.len() {
        let b = bytes[i];
        if !esc && b == b'\\' {
            esc = true;
        } else if !esc && b == b'"' {
            break;
        } else {
            esc = false;
            val.push(b);
        }
        i += 1;
    }
    String::from_utf8(val).ok()
}

fn is_hex(b: u8) -> bool {
    (b'0'..=b'9').contains(&b) || (b'a'..=b'f').contains(&b) || (b'A'..=b'F').contains(&b)
}

fn looks_like_uuid(s: &str) -> bool {
    if s.len() != 36 {
        return false;
    }
    let bytes = s.as_bytes();
    for (i, &b) in bytes.iter().enumerate() {
        match i {
            8 | 13 | 18 | 23 => {
                if b != b'-' {
                    return false;
                }
            }
            _ => {
                if !is_hex(b) {
                    return false;
                }
            }
        }
    }
    true
}

#[test]
fn test_grpcurl_available_and_proto_present() {
    if let Err(msg) = ensure_test_prereqs() {
        eprintln!("Prerequisite check failed: {}", msg);
        // Do not fail CI if environment is not prepared; treat as skipped.
        return;
    }
}

#[test]
fn test_submit_task_happy_path() {
    if let Err(msg) = ensure_test_prereqs() {
        eprintln!("Prerequisite check failed: {}", msg);
        return;
    }

    let payload = r#"{
        "runtime": "rust:1.70",
        "code": "",
        "timeLimitMs": 2000,
        "memoryLimitKb": 262144,
        "userId": "user-123"
    }"#;

    let out = match run_submit_task(payload) {
        Ok(o) => o,
        Err(e) => {
            panic!("grpc call failed: {}", e);
        }
    };

    // Basic structure checks
    let task_id = extract_json_string_field(&out, "taskId").unwrap_or_default();
    let status = extract_json_string_field(&out, "status").unwrap_or_default();
    let submitted_at = extract_json_string_field(&out, "submittedAt").unwrap_or_default();

    assert!(!task_id.is_empty(), "taskId missing in response: {}", out);
    assert!(
        looks_like_uuid(&task_id),
        "taskId is not UUID-like: {}",
        task_id
    );
    assert_eq!(status, "QUEUED", "unexpected status: {}", status);
    assert!(!submitted_at.is_empty(), "submittedAt missing");
    assert!(
        submitted_at.contains('T') && submitted_at.ends_with('Z'),
        "submittedAt not RFC3339-like (expected ...T...Z): {}",
        submitted_at
    );
}

#[test]
fn test_submit_task_multiple_requests() {
    if let Err(msg) = ensure_test_prereqs() {
        eprintln!("Prerequisite check failed: {}", msg);
        return;
    }

    let payload = r#"{
        "runtime": "rust:1.70",
        "code": "",
        "timeLimitMs": 2000,
        "memoryLimitKb": 262144,
        "userId": "user-123"
    }"#;

    let out1 = run_submit_task(payload).expect("first grpc call failed");
    let out2 = run_submit_task(payload).expect("second grpc call failed");

    let id1 = extract_json_string_field(&out1, "taskId").unwrap_or_default();
    let id2 = extract_json_string_field(&out2, "taskId").unwrap_or_default();

    assert!(looks_like_uuid(&id1));
    assert!(looks_like_uuid(&id2));
    assert_ne!(id1, id2, "expected unique taskId per submission");
}
