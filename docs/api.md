# REST 与 Temporal API

本文档描述当前实现的公开契约。管理 API 默认监听 `http://127.0.0.1:8080`，所有时间使用 RFC 3339，duration 使用 Go duration 格式，例如 `500ms`、`10s`、`5m`。

- OpenAPI 3.1：`GET /openapi.yaml`
- Swagger UI：`GET /docs`

Swagger UI 的静态资源来自 `unpkg.com`；OpenAPI YAML 内嵌在 binary 中，可以离线获取。

## REST 管理 API

### 通用规则

- 服务 ID 必须匹配 `^[a-z0-9][a-z0-9_-]{0,127}$`。
- `PUT` 请求必须使用 `Content-Type: application/json` 或 `application/*+json`。
- ServiceSpec 请求体最大 4 MiB，未知 JSON 字段会被拒绝。
- 成功响应使用 JSON。
- `GET /services` 当前返回完整数组，没有分页参数。

错误响应统一为：

```json
{
  "error": {
    "code": "invalid_request",
    "message": "..."
  }
}
```

常见状态码：

| HTTP status | code | 含义 |
|---|---|---|
| `400` | `invalid_request` | service ID、配置、schema 或 JSON 无效 |
| `404` | `not_found` | 服务不存在 |
| `415` | `unsupported_media_type` | PUT 没有使用 JSON Content-Type |
| `500` | `internal` | SQLite、Temporal 或内部状态操作失败 |
| `503` | `unavailable` | `/healthz` 检查失败 |

## ServiceSpec

完整请求示例：

```json
{
  "actions": {
    "invoke": {
      "method": "POST",
      "endpoint": "https://backend.example.com/runsync",
      "request_schema": {
        "type": "object",
        "required": ["prompt"]
      },
      "response_schemas": {
        "200": {
          "type": "object",
          "required": ["job_id"]
        }
      }
    },
    "status": {
      "method": "GET",
      "endpoint": "https://backend.example.com/jobs/{job_id}"
    }
  },
  "health": {
    "method": "GET",
    "endpoint": "https://backend.example.com/health",
    "timeout": "3s"
  },
  "timeout": "5m"
}
```

顶层字段：

| 字段 | 必填 | 说明 |
|---|---:|---|
| `actions` | 是 | 非空 action map。名称匹配 `^[a-z0-9][a-z0-9._-]{0,127}$` |
| `timeout` | 是 | 每次业务 HTTP Activity 的正 Go duration |
| `health` | 否 | 独立的后端 HTTP 健康探针，不注册为 Temporal Activity |

ActionSpec：

| 字段 | 必填 | 说明 |
|---|---:|---|
| `method` | 是 | HTTP method，保存时规范化为大写 |
| `endpoint` | 是 | 绝对 `http` 或 `https` URL |
| `request_schema` | 否 | JSON Schema 2020-12。仅当请求 Content-Type 是 JSON 时校验 |
| `response_schemas` | 否 | 按精确十进制 HTTP status 配置的 JSON Schema map，例如 `"200"` |

`endpoint` 可以在 URL path 中使用 `{job_id}`。调用时必须提供 `Request.JobID`，Gateway 会将它作为单个 path segment 转义。query、fragment 或其他占位符不支持。

HealthSpec：

| 字段 | 必填 | 说明 |
|---|---:|---|
| `method` | 是 | health 请求使用的 HTTP method |
| `endpoint` | 是 | 绝对 URL，不允许 `{job_id}` |
| `timeout` | 是 | health 请求独立使用的正 Go duration |

health 请求没有 body，也不会自动附加业务 Activity 的鉴权 header。

规范化后的配置会进入 revision hash。method 大小写、endpoint 两侧空格和等价 duration 不会制造无意义 revision；health、action、schema 或 timeout 的有效变化会创建新 revision。

## PUT /services/{id}

新增、更新或恢复服务。成功返回前，Gateway 已经：

1. 校验并规范化 ServiceSpec。
2. 在 SQLite 创建 starting revision。
3. 启动新的 Activity-only Worker。
4. 将对应 Worker Deployment Version 切成 Current。
5. 激活 revision 并排空旧 Worker。

请求：

```bash
curl --fail-with-body \
  -X PUT http://127.0.0.1:8080/services/demo-service \
  -H 'Content-Type: application/json' \
  --data-binary @service.json
```

成功：`200 OK`

```json
{
  "id": "demo-service",
  "state": "active",
  "current_revision": 1,
  "revision_state": "active",
  "revision_kind": "service",
  "latest_revision": 1,
  "latest_revision_state": "active",
  "spec": {
    "actions": {
      "invoke": {
        "method": "POST",
        "endpoint": "https://backend.example.com/runsync"
      }
    },
    "timeout": "10s"
  },
  "created_at": "2026-11-01T12:00:00Z",
  "updated_at": "2026-11-01T12:00:00Z"
}
```

提交与当前配置相同的规范化结果仍返回 `200`，但不会创建新 revision。

## GET /services

返回所有服务，包括 deleted 服务，按 service ID 排序。实现通过单条批量 JOIN 同时读取 service、current revision 和 latest revision。

```bash
curl --fail-with-body http://127.0.0.1:8080/services
```

成功：`200 OK`

```json
[
  {
    "id": "demo-service",
    "state": "active",
    "current_revision": 1,
    "revision_state": "active",
    "revision_kind": "service",
    "latest_revision": 1,
    "latest_revision_state": "active",
    "spec": {
      "actions": {
        "invoke": {
          "method": "POST",
          "endpoint": "https://backend.example.com/runsync"
        }
      },
      "timeout": "10s"
    },
    "created_at": "2026-11-01T12:00:00Z",
    "updated_at": "2026-11-01T12:00:00Z"
  }
]
```

## GET /services/{id}

读取单个服务的 current 和 latest revision 状态。服务已删除时仍返回 `200`，但 `state` 为 `deleted`，且 tombstone 不包含 `spec`。

```bash
curl --fail-with-body http://127.0.0.1:8080/services/demo-service
```

不存在：`404 not_found`。

## GET /services/{id}/health

查询 Registry current revision 配置的后端 health endpoint。它不会查询 Temporal poller，也不会把结果写入 SQLite。

```bash
curl --fail-with-body http://127.0.0.1:8080/services/demo-service/health
```

响应字段：

| 字段 | 说明 |
|---|---|
| `service_id` | 服务 ID |
| `revision` | 被检查的 current revision |
| `registry_state` | SQLite 中的 service state |
| `health` | `healthy`、`unhealthy`、`not_configured` 或 `not_ready` |
| `http_status` | 后端返回的最终 HTTP status；网络错误时省略 |
| `latency_ms` | Gateway 观察到的请求延迟，毫秒浮点数 |
| `message` | HTTP status text 或网络错误说明 |
| `checked_at` | 检查完成时间 |

状态映射：

| 条件 | API status | `health` |
|---|---:|---|
| 后端最终返回任意 2xx | `200` | `healthy` |
| 未配置 health | `200` | `not_configured` |
| 后端非 2xx、网络错误或超时 | `503` | `unhealthy` |
| service state 不是 active | `503` | `not_ready` |
| service 不存在 | `404` | 错误 envelope |

每次探针都会写一条 `service health checked` 结构化日志，包含 service ID、revision、Registry state、health、HTTP status、latency 和 message。

## DELETE /services/{id}

创建新的 tombstone revision 并将其切为 Current Version。已经开始的旧 Activity 允许完成；尚未开始的任务会进入 tombstone 并返回 `ServiceDeleted`。

```bash
curl --fail-with-body \
  -X DELETE http://127.0.0.1:8080/services/demo-service
```

成功：`200 OK`，响应中的 `state` 为 `deleted`、`revision_kind` 为 `tombstone`。重复 DELETE 是 no-op。不存在的服务返回 `404`。

重新 `PUT` 相同 service ID 会创建更高 revision 并恢复服务。

## GET /healthz

检查 SQLite 和 Temporal Frontend 连接，不检查各服务的 HTTP 后端。

```bash
curl --fail-with-body http://127.0.0.1:8080/healthz
```

成功：

```json
{"status":"ok"}
```

失败：`503 unavailable`。

## Service 和 revision 状态

Service state：

| 状态 | 含义 |
|---|---|
| `starting` | 首个 revision 尚未成为 Current |
| `active` | current revision 是普通 service revision |
| `deleted` | current revision 是 tombstone |
| `failed` | 首个 revision 明确失败，当前没有可用 service revision |

Revision state：

| 状态 | 含义 |
|---|---|
| `starting` | Worker 启动或 Current Version 切换仍在进行，结果可能未知 |
| `active` | Temporal 正把新任务路由到该 revision |
| `draining` | 已被新 revision 替换，正在排空已开始的 Activity |
| `failed` | Worker 启动或切流已被明确判定失败 |

## Temporal Activity API

调用方不依赖 temporaless 客户端库，只依赖所用语言的官方 Temporal SDK。服务注册、查询和删除使用本页描述的普通 REST API。

调用方使用稳定名称：

| 项目 | 值 |
|---|---|
| Task Queue | `{service_id}` |
| Activity Type | `{service_id}/invoke` |
| Worker Deployment | `{service_id}` |
| Build ID | `r{revision}`，只由 Gateway 管理 |

Activity 接收一个 JSON 参数：

```json
{
  "action": "invoke",
  "headers": {
    "Authorization": ["Bearer token"],
    "Content-Type": ["application/json"],
    "Idempotency-Key": ["op-123"]
  },
  "body": "eyJwcm9tcHQiOiJoZWxsbyJ9",
  "job_id": "optional-job-id"
}
```

语言无关字段：

| 字段 | JSON 类型 | 说明 |
|---|---|---|
| `action` | string | 必须匹配 ServiceSpec 中的 action 名称 |
| `headers` | object of string arrays | 端到端 HTTP header。hop-by-hop header 会被过滤 |
| `body` | Base64 string | 原始 HTTP body bytes。示例解码后是 `{"prompt":"hello"}` |
| `job_id` | string | endpoint 含 `{job_id}` 时必填，并按 path segment 转义 |

Go 调用方可以在自己的代码中声明等价 DTO，`encoding/json` 会自动处理 `[]byte` 的 Base64：

```go
type GatewayRequest struct {
    Action  string
    Headers http.Header
    Body    []byte
    JobID   string
}
```

Activity 返回一个 JSON object：

```json
{
  "status": 200,
  "message": "OK",
  "headers": {
    "Content-Type": ["application/json"]
  },
  "body": "eyJqb2JfaWQiOiJvcC0xMjMiLCJzdGF0dXMiOiJjb21wbGV0ZWQifQ=="
}
```

等价 Go DTO：

```go
type GatewayResponse struct {
    Status  int
    Message string
    Headers http.Header
    Body    []byte
}
```

其他语言应使用官方 SDK 的 JSON Payload Converter，把上述对象作为唯一 Activity 参数。不要传语言私有的二进制对象或自定义 codec，除非 Gateway Worker 配置了匹配的 Data Converter。

HTTP 4xx 和 5xx 仍是正常 Activity 结果。调用方必须检查 `Response.Status`。以下情况是 Activity failure：

- DNS、连接和 HTTP timeout。
- redirect 失败。
- Gateway 内部读取失败。

以下是 non-retryable Temporal Application Error：

| Type | 条件 |
|---|---|
| `ActionNotFound` | action 未配置 |
| `InvalidRequest` | endpoint 需要 JobID，但请求没有提供 |
| `RequestSchemaMismatch` | JSON 请求不符合 request schema |
| `ResponseSchemaMismatch` | JSON 响应不符合对应 status schema |
| `ResponseTooLarge` | 响应超过 `max-backend-response-bytes` |
| `ServiceDeleted` | 当前 revision 是 tombstone |

推荐 Activity Options：

```go
workflow.ActivityOptions{
    TaskQueue:              serviceID,
    ScheduleToCloseTimeout: 30 * time.Second,
    StartToCloseTimeout:    15 * time.Second,
    HeartbeatTimeout:       2 * time.Second,
    WaitForCancellation:    true,
    RetryPolicy: &temporal.RetryPolicy{
        MaximumAttempts: 3,
    },
}
```

Gateway 周期性 heartbeat，使 Workflow cancellation 能传播到 Go context 和正在进行的 HTTP request。`HeartbeatTimeout` 决定取消检测延迟；`WaitForCancellation` 决定 Workflow 是否等待 Activity 确认退出。

完整可运行调用见 [examples/caller](../examples/caller/main.go)。

## Gateway YAML 配置

启动命令只有一个可选参数：

```bash
temporaless -config /etc/temporaless/config.yaml
```

省略 `-config` 时使用以下内建默认值。仓库中的 [temporaless.example.yaml](../temporaless.example.yaml) 可直接用于 Quick Start，其中 `registry.path` 改为 `/tmp/temporaless-quickstart.db`，其余字段与默认值一致。

```yaml
server:
  listen: 127.0.0.1:8080
  read_header_timeout: 5s
  read_timeout: 15s
  idle_timeout: 1m
  shutdown_timeout: 10s

registry:
  path: temporaless.db

temporal:
  address: localhost:7233
  namespace: default
  startup_timeout: 2m

worker:
  switch_timeout: 30s
  stop_timeout: 30s
  max_concurrent_activities_per_service: 32

http:
  max_backend_response_bytes: 1048576
  max_idle_connections_per_host: 32
```

参数说明：

| YAML path | 默认值 | 说明 |
|---|---|---|
| `server.listen` | `127.0.0.1:8080` | REST、OpenAPI 和 Swagger UI 的监听地址 |
| `server.read_header_timeout` | `5s` | 读取 HTTP request headers 的最长时间 |
| `server.read_timeout` | `15s` | 读取完整管理请求的最长时间 |
| `server.idle_timeout` | `1m` | keep-alive 空闲连接超时 |
| `server.shutdown_timeout` | `10s` | 收到进程退出信号后等待 HTTP server 关闭的时间 |
| `registry.path` | `temporaless.db` | SQLite 文件路径；`:memory:` 表示内存数据库 |
| `temporal.address` | `localhost:7233` | Temporal Frontend 地址 |
| `temporal.namespace` | `default` | Gateway Workers 所在 Namespace |
| `temporal.startup_timeout` | `2m` | 启动 reconcile 的总 deadline |
| `worker.switch_timeout` | `30s` | 单次 revision Current Version 切换 deadline |
| `worker.stop_timeout` | `30s` | 旧 Worker 排空 in-flight Activity 的宽限时间 |
| `worker.max_concurrent_activities_per_service` | `32` | 每个服务 Worker 的 Activity 并发上限 |
| `http.max_backend_response_bytes` | `1048576` | 读入内存并返回 Temporal 的最大后端响应体 bytes |
| `http.max_idle_connections_per_host` | `32` | 共享 HTTP Transport 每个 host 保留的最大空闲连接数 |

YAML 支持只写需要覆盖的字段，其他字段保留默认值。未知字段、多个 YAML document、空地址、非正 duration 和非正数值都会导致进程在启动前报错。

## 安全边界

- 管理 API 没有内建鉴权，默认只监听 loopback。生产环境必须在入口增加鉴权。
- 能注册任意 HTTP endpoint 等同于受控 SSRF/出网能力，生产环境必须配置 endpoint/egress 网络策略。
- Activity header 和 body 会进入 Temporal Event History。真实凭据必须使用 Payload Codec 加密，或改为 secret reference。
- Gateway 日志不记录 Activity header 和 body；health 日志只记录状态、latency 和错误说明。
