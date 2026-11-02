# Quick Start

这份指南从空环境跑通一条真实链路：Go Workflow 调度 Temporal Activity，`temporaless` Worker 收到 Activity 后调用 HTTP 后端，再把完整 HTTP 响应交还 Workflow。

```text
examples/caller Workflow
        │ ExecuteActivity("demo-service/invoke")
        ▼
Temporal Task Queue: demo-service
        │
        ▼
temporaless Worker Deployment: demo-service / r1
        │ HTTP POST /runsync
        ▼
examples/backend
```

## 前置条件

- Go 1.25.4 或更高版本。
- Temporal CLI，能够运行 `temporal server start-dev`。
- 当前目录是 `temporaless` 仓库根目录。

首次运行 Go 命令时会下载 `go.mod` 中的依赖。

## 1. 启动 Temporal

终端一：

```bash
temporal server start-dev
```

本地默认地址：

- Temporal Frontend：`localhost:7233`
- Temporal UI：<http://localhost:8233>

Quick Start 使用 `default` Namespace。

## 2. 启动示例 HTTP 后端

终端二：

```bash
go run ./examples/backend
```

后端监听 `127.0.0.1:18080`，提供：

- `GET /health`：返回 `204 No Content`。
- `POST /runsync`：要求 `Authorization` 和 `Idempotency-Key`，接收 JSON 请求并同步返回结果。
- `GET /jobs/{id}`：返回示例任务状态。

可以先直接确认后端在线：

```bash
curl -i http://127.0.0.1:18080/health
```

## 3. 启动 temporaless

终端三：

```bash
go run ./cmd/temporaless \
  -config temporaless.example.yaml
```

看到下面两条 JSON 日志表示管理 API 和可选 metrics server 已启动：

```json
{"time":"2026-11-02T10:00:00Z","level":"INFO","msg":"control API listening","service":"temporaless","address":"127.0.0.1:8080"}
{"time":"2026-11-02T10:00:00Z","level":"INFO","msg":"metrics listening","service":"temporaless","address":"127.0.0.1:9090"}
```

REST 契约可以直接查看：

- Swagger UI：<http://127.0.0.1:8080/docs>
- OpenAPI YAML：<http://127.0.0.1:8080/openapi.yaml>

示例配置还在独立端口开放 Prometheus 指标：

```bash
curl --fail http://127.0.0.1:9090/metrics
```

## 4. 注册 HTTP 服务

终端四：

```bash
curl --fail-with-body \
  -X PUT http://127.0.0.1:8080/services/demo-service \
  -H 'Content-Type: application/json' \
  -d '{
    "actions": {
      "invoke": {
        "method": "POST",
        "endpoint": "http://127.0.0.1:18080/runsync",
        "request_schema": {
          "type": "object",
          "required": ["prompt"],
          "properties": {
            "prompt": {"type": "string", "minLength": 1}
          },
          "additionalProperties": false
        },
        "response_schemas": {
          "200": {
            "type": "object",
            "required": ["job_id", "status", "output"]
          }
        }
      },
      "status": {
        "method": "GET",
        "endpoint": "http://127.0.0.1:18080/jobs/{job_id}"
      }
    },
    "health": {
      "method": "GET",
      "endpoint": "http://127.0.0.1:18080/health",
      "timeout": "3s"
    },
    "timeout": "10s"
  }'
```

成功响应中的关键字段：

```json
{
  "id": "demo-service",
  "state": "active",
  "current_revision": 1,
  "revision_state": "active",
  "revision_kind": "service",
  "latest_revision": 1,
  "latest_revision_state": "active"
}
```

这一步完成后，Temporal 中已经存在：

- Task Queue：`demo-service`
- Activity Type：`demo-service/invoke`
- Worker Deployment：`demo-service`
- Current Build ID：`r1`

重复提交规范化后完全相同的配置是 no-op，不会创建 `r2`。

## 5. 检查服务健康状态

```bash
curl --fail-with-body http://127.0.0.1:8080/services/demo-service/health
```

响应示例：

```json
{
  "service_id": "demo-service",
  "revision": 1,
  "registry_state": "active",
  "health": "healthy",
  "http_status": 204,
  "latency_ms": 0.82,
  "message": "No Content",
  "checked_at": "2026-11-01T12:00:00Z"
}
```

Gateway 同时输出一条 `service health checked` 结构化日志，包含 `service_id`、revision、HTTP status 和 `latency_ms`。health 探针只检查配置的 HTTP 后端，不检查 Temporal poller。

## 6. 从真实 Workflow 调用 Gateway Activity

终端五：

```bash
go run ./examples/caller
```

这个程序会：

1. 连接 `localhost:7233/default`。
2. 启动一个只负责 Quick Start Workflow 的 Worker。
3. 执行 `temporaless-quickstart-invoke` Workflow。
4. Workflow 将 `demo-service/invoke` Activity 调度到 `demo-service` Task Queue。
5. temporaless 将 HTTP 请求发到示例后端。

预期输出：

```text
workflow_id=quickstart-... run_id=...
status=200 message="OK" content_type="application/json"
body={"job_id":"quickstart-...","output":"generated: a small robot playing piano","status":"completed"}
```

真实的下游 Temporal 调用在 [examples/caller/main.go](../examples/caller/main.go) 中。它只导入官方 `go.temporal.io/sdk`，在调用方本地声明 JSON DTO，不依赖 `temporaless` module。核心部分是：

```go
ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
    TaskQueue:              input.ServiceID,
    ScheduleToCloseTimeout: 30 * time.Second,
    StartToCloseTimeout:    15 * time.Second,
    HeartbeatTimeout:       2 * time.Second,
    WaitForCancellation:    true,
})

var response gatewayResponse
err := workflow.ExecuteActivity(
    ctx,
    input.ServiceID+"/invoke",
    input.Request,
).Get(ctx, &response)
```

调用方只使用稳定的 service ID，不读取 SQLite revision，也不拼接 Build ID。

### 其他语言调用

任何官方 Temporal SDK 都可以调用，只需把一个 JSON 对象作为唯一 Activity 参数调度到正确 Task Queue：

```json
{
  "action": "invoke",
  "headers": {
    "Authorization": ["Bearer demo-token"],
    "Content-Type": ["application/json"],
    "Idempotency-Key": ["op-123"]
  },
  "body": "eyJwcm9tcHQiOiJoZWxsbyJ9"
}
```

- Activity Type：`demo-service/invoke`
- Task Queue：`demo-service`
- Activity 参数个数：一个 JSON object
- `headers`：`string -> string[]`
- `body`：原始 HTTP bytes 的标准 Base64；示例值解码后是 `{"prompt":"hello"}`
- `job_id`：只有 endpoint 使用 `{job_id}` 时才需要

响应也是 JSON object，`body` 同样是 Base64。各语言只需要官方 SDK 的 JSON Payload Converter 和标准 Base64/HTTP 库。

## 7. 在 Temporal UI 中确认链路

打开 <http://localhost:8233>，找到 `quickstart-...` Workflow。Event History 中应能看到：

- Workflow Task Queue：`temporaless-quickstart-caller`
- Activity Task Queue：`demo-service`
- Activity Type：`demo-service/invoke`
- Activity 输入中的 action、header 和 body
- Activity 输出中的 status、message、header 和 body

注意：默认 Data Converter 会把这些 payload 写入 Temporal Event History。生产凭据必须配置 Payload Codec 加密，或改为 secret reference。

## 8. 更新和删除

修改 endpoint、schema、timeout 或 health 后重新 `PUT`，Gateway 会创建 `r2`，启动新 Worker，切换 Current Version，再排空 `r1`。

删除服务：

```bash
curl --fail-with-body \
  -X DELETE http://127.0.0.1:8080/services/demo-service
```

删除会创建 tombstone revision。后续仍调度 `demo-service/invoke` 的调用会快速得到 non-retryable `ServiceDeleted`，而不是永久等待没有 Worker 的 Task Queue。

重新提交 `PUT /services/demo-service` 可以用更高 revision 恢复服务。

## 常见问题

### 注册服务返回 500

先确认 `localhost:7233` 可连接，并查看 Gateway 日志。注册成功前需要启动新 Worker 并完成 Worker Deployment Current Version 切换。

### health 返回 503

响应中的 `health` 会区分 `unhealthy` 和 `not_ready`：

- `unhealthy`：后端返回非 2xx、网络错误或 health timeout。
- `not_ready`：Registry 中服务不是 active。
- `not_configured`：没有 health 配置，接口返回 200。

### Activity 一直重试

网络错误和超时是 Activity failure，会按调用方配置的 Retry Policy 重试。HTTP 4xx/5xx 是正常的 Gateway JSON response，不会自动转成 Activity failure。

### 响应过大

后端响应体默认限制为 1 MiB，超限返回 non-retryable `ResponseTooLarge`。可在 YAML 中调整 `http.max_backend_response_bytes`，但大对象更适合返回对象存储引用。

完整字段和状态码见 [REST 与 Temporal API](api.md)。架构和失败恢复语义见 [ADR-0001](adr/0001-hot-reloadable-http-activity-workers.md)。
