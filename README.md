# molaoj

a super simple and fast oj system.

## Basic Usage

1. 使用 cargo 工具启动 mola-dispatcher 和 mola-worker

``` shell
cargo run --bin mola-dispatcher 
cargo run --bin mola-worker 
```

1. 使用 docker compose 工具启动 rabbitmq 和 postgresql 服务

``` shell
docker compose up
```

1. 使用 grpcurl 工具测试功能

``` shell
grpcurl -plaintext -import-path proto -proto proto/task.proto -d '{"runtime": "bash","code": "/usr/bin/echo hello", "compile_cmd": "", "run_cmd": "/usr/bin/bash","timeLimitMs": 2000,"memoryLimitKb": 262144,"userId": "user-123"}' localhost:50051 task.v1.JudgeDispatcher/SubmitTask
```

## TODO

- 完整实现 grpc 调用函数
- 实现 rabbitmq 消息推送函数
- 实现 rabbitmq 消息消耗函数
- 实现 grpc 和 rabbitmq 流传输功能
- 添加判题功能 (proto, grpc_service...)
- 实现 worker 评测功能
- 给 dispatcher 和 worker 打包 docker 镜像，编写打包脚本
- 为 worker 添加 docker 和 nsjail 隔离
- 使用 k8s 等工具为 worker 实现动态扩容
- 为 grpc 和 rabbitmq 添加 tls 加密
- 为 postgresql 和 rabbitmq 实现连接池功能
- 添加 prometheus 监控端点
- 添加配置文件读取功能
- 添加前端演示界面
- 完善相关测试和文档
