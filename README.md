# temporaless

`temporaless` 是一个 Go 编写的 Temporal Activity Worker Gateway。每个注册的 HTTP 服务拥有稳定的 Activity Type、Task Queue 和 Worker Deployment，通过 revision Build ID 实现热新增、热更新和 tombstone 热删除。

## 启动

先启动本地 Temporal，然后运行：

```bash
go run ./cmd/temporaless \
  -listen 127.0.0.1:8080 \
  -db temporaless.db \
  -temporal-address localhost:7233 \
  -temporal-namespace default
```

默认只监听 loopback。生产使用前需要为管理 API 增加鉴权，并处理 Temporal History 中 payload 的加密或 secret reference。

## 注册服务

服务 ID 必须匹配 `^[a-z0-9][a-z0-9_-]{0,127}$`。

```bash
curl -X PUT http://127.0.0.1:8080/services/runpod-image \
  -H 'Content-Type: application/json' \
  -d '{
    "actions": {
      "invoke": {
        "method": "POST",
        "endpoint": "https://example.com/runsync",
        "request_schema": {"type": "object"},
        "response_schemas": {
          "200": {"type": "object"}
        }
      },
      "status": {
        "method": "GET",
        "endpoint": "https://example.com/jobs/{job_id}"
      }
    },
    "timeout": "5m"
  }'
```

相同规范化配置的重复 `PUT` 不产生新 revision。更新配置会先启动新 Worker、切换 Current Version，再优雅停止旧 Worker。

管理接口：

- `PUT /services/{id}`：新增、恢复或更新服务。
- `GET /services`：列出服务。
- `GET /services/{id}`：读取服务及 revision 状态。
- `DELETE /services/{id}`：切换到返回 `ServiceDeleted` 的 tombstone revision。
- `GET /healthz`：检查 SQLite 与 Temporal。

## 调度 Activity

调用方始终使用：

- Activity Type：`{service_id}/invoke`
- Task Queue：`{service_id}`

Activity payload：

```go
temporaless.Request{
    Action: "invoke",
    Headers: http.Header{
        "Authorization":   {"Bearer ..."},
        "Idempotency-Key": {operationID},
    },
    Body: rawBody,
}
```

响应使用 Go `net/http` 暴露的 status、header 和 body 构造 `temporaless.Response`。Gateway 不解析或重组业务对象；4xx/5xx 是正常 Activity 结果，网络错误、连接错误和超时才是 Activity failure。配置 schema 时，仅对 JSON body 的副本做校验。这里承诺的是 HTTP 对象语义透明，不是 wire bytes 透明：redirect、gzip 解压和 framing 等仍按 `net/http` 标准行为处理。

Gateway 会在持续执行的 HTTP Activity 中发送空 heartbeat，使 Workflow cancellation 能取消 Activity context 和正在进行的 HTTP request。调用 Workflow 应按期望的取消延迟设置 `HeartbeatTimeout`；只有需要等待 HTTP 调用实际退出时才设置 `WaitForCancellation: true`，默认值 `false` 只发出取消请求而不等待确认。

## 验证

```bash
go test ./...
TEMPORAL_SMOKE=1 go test ./...
```

第二条命令需要 `localhost:7233` 上运行 Temporal，覆盖 Worker Deployment backlog、并发 in-flight Activity 排空、Activity heartbeat 取消传播以及完整的 r1 → r2 → tombstone 调用链。

架构与失败语义见 [ADR-0001](docs/adr/0001-hot-reloadable-http-activity-workers.md)。
