# ADR-0001：以版本化 Activity Worker 实现可热更新的 HTTP 服务

- 状态：已接受
- 日期：2026-10-31

## 背景

本项目提供一个 Go Gateway，作为 Temporal Worker 调用 Runpod、阿里云函数计算等 HTTP 服务。它需要通过 REST API 动态新增、更新和删除服务，并在不解释业务 payload 的前提下转发请求和响应。后端负责鉴权与幂等，Gateway 只透传相应 header。

首版运行在单个长驻进程中，Temporal Server 为本地 1.32.0，Registry 使用 SQLite。

Temporal 1.32 引入的 Serverless Workers 由 Worker Controller Instance（WCI）监控 Task Queue，再调用 compute provider 启动承载 Temporal Worker 的计算资源。当前官方 provider 是 AWS Lambda、AWS Bedrock AgentCore 和 GCP Cloud Run。它解决的是“谁来启动 Worker”，不是“Activity 如何调用任意 serverless HTTP 后端”，因此不能直接替代本 Gateway。

Serverless Workers 强制使用 Worker Deployment Versioning，其中可复用的原则是：Deployment 表示逻辑服务、Build ID 表示不可变版本，先让新版本就绪，再切换 Current Version，旧版本继续排空。

## 决策

### 1. 一个服务对应一个稳定 Activity

每个服务使用以下 Temporal 名称，不增加 `gateway/` 前缀：

- Activity Type：`{service_id}/invoke`
- Task Queue：`{service_id}`
- Worker Deployment：`{service_id}`
- Build ID：`r{revision}`，例如 `r17`

`service_id` 必须匹配 `^[a-z0-9][a-z0-9_-]{0,127}$`。Worker Deployment Name 不允许包含 `.`，因此首版不接受点号，避免引入另一套不可见的名称编码规则。

一个配置 revision 启动一个 Activity-only Worker。Worker 必须设置 `DisableWorkflowWorker: true`，使用相同的稳定 Task Queue 和 Activity Type，并通过 `DeploymentOptions` 报告 Deployment Name 与 Build ID。不同 revision 的 Activity closure 捕获各自不可变的 HTTP action、endpoint、schema 和 timeout。

调用方始终调度 `{service_id}/invoke` 到 `{service_id}`，不感知 revision。

### 2. 使用 Worker Deployment Versioning 路由 revision

新 revision 的切换顺序为：

1. 在 SQLite 写入 `starting` revision。
2. 启动 `Build ID = rN` 的 Worker；`SetCurrentVersion` 默认拒绝从未出现过 poller 的版本，因此直接复用 Temporal 的就绪保护。
3. 在 SQLite 持久化 `switch_started = true`，表示之后可能存在仍在服务端完成的切流操作。
4. 将该 Worker Deployment Version 设置为 Current Version。
5. 在 SQLite 中将新 revision 标记为 `active`，旧 revision 标记为 `draining`。
6. 调用旧 Worker 的 graceful `Stop`：停止新 poll，并在 `WorkerStopTimeout` 内等待已经开始的 Activity 完成。

新 Worker 启动失败或 Temporal 明确拒绝且没有更早的未决切流时，Current Version 保持不变，新 revision 标记为 `failed`。RPC 超时、连接中断以及它们之后观察到的旧 Current 都不能证明切流失败：服务端可能仍在完成已接受的 Worker Deployment Update。此时保留 `starting` revision、`switch_started` 和新旧 Worker，不强制写回 SQLite 中的旧版本。启动 reconcile 会先预热 SQLite 记录的当前 Worker，再恢复未完成 revision；已成功替换旧 Current 时忽略旧版本的预热错误。已经可靠落成 `failed` 的单服务 revision 不阻止其他服务启动，未知路由或无法恢复当前版本仍作为进程级错误。进程在任一步骤崩溃后，以 Temporal 当前路由和 SQLite revision 记录继续收敛。

因此不再引入 `resolve-service` Activity，也不创建 `{service_id}/rN` Task Queue。Temporal 对独立 Activity 按其 Task Queue 的 Current/Ramping Version 路由，避免应用层“先解析 revision、后调度 Activity”的竞态。

本地 Server 1.32.0 冒烟验证表明，尚未开始的独立 Activity backlog 会在切换后跟随新的 Current Version，不固定在入队时的版本；只有已经开始的 Activity 留在旧 Worker。因此旧版本排空以 Worker graceful stop 和本地 in-flight 为准，不等待旧版本的服务端 backlog。

首版不启用 Ramping Version，只做原子全量切换；灰度发布不是当前需求。

### 3. 所有后端统一为 HTTP

不保留 `transport` 字段，也不实现 Runpod、阿里云 FC 等 provider SDK adapter。一个服务可声明多个纯 HTTP action：

```json
{
  "actions": {
    "invoke": {
      "method": "POST",
      "endpoint": "https://example.com/runsync",
      "request_schema": {},
      "response_schemas": {
        "200": {}
      }
    },
    "status": {
      "method": "GET",
      "endpoint": "https://example.com/status/{job_id}"
    }
  },
  "health": {
    "method": "GET",
    "endpoint": "https://example.com/health",
    "timeout": "3s"
  },
  "timeout": "5m"
}
```

`invoke`、`submit`、`status`、`cancel` 只是普通 action 名称，不带 provider 语义。每个 action 独立配置 method、endpoint 和 schema；`response_schemas` 按十进制 HTTP status 精确匹配。需要异步任务 ID 的 endpoint 只允许使用 `{job_id}` 占位符，替换时按单个 URL path segment 转义。未配置的 action 直接返回 non-retryable `ActionNotFound`。

`health` 是独立于业务 Activity 的可选 HTTP 探针，配置 method、endpoint 和独立 timeout，不允许 `{job_id}`。`GET /services/{id}/health` 直接从 Gateway 请求后端，任意 2xx 表示 healthy；未配置时返回 `not_configured`，非 2xx、网络错误或超时返回 unhealthy。结果不写入 SQLite，响应和结构化日志记录 HTTP status 与 latency，供外部可观测系统查询。该接口不查询 Temporal poller，Worker/Temporal 可用性属于 Gateway 基础设施健康。

### 4. payload 保持 HTTP 对象语义透明

Activity 输入输出为最小 HTTP envelope：

```go
type Request struct {
	Action  string
	Headers map[string][]string
	Body    []byte
	JobID   string
}

type Response struct {
	Status  int
	Message string
	Headers map[string][]string
	Body    []byte
}
```

- method、endpoint 和 schema 来自 immutable revision 中对应的 action。Gateway 不解析、映射或重组业务对象。
- 仅在 request 是 JSON 且 action 配置了 `request_schema` 时，解析 request body 的副本进行 JSON Schema 2020-12 校验；交给 `net/http` 的仍是 Activity 输入中的 body。
- Gateway 不从响应中提取 Job ID；调用方从 HTTP response body 读取后，在后续 action 的 `JobID` 字段传回。
- `Message` 是 Gateway 生成的简短可读说明。收到 HTTP response 时使用 `http.StatusText(Status)`；不从 body 猜测或提取后端错误消息，完整信息仍在 `Body` 中。
- 透传端到端 header，包括后端所需的鉴权和幂等键；移除 `Connection`、`Transfer-Encoding`、`Content-Length` 等 hop-by-hop/传输层 header，由 Go HTTP client 重建。
- HTTP client 按 Go `net/http` 标准语义跟随 redirect，最多 10 次，并返回最终响应。跨 host redirect 的敏感 header 遵循 `net/http` 的安全转发规则，不由 Gateway 重新复制；超过上限或 redirect 失败作为 Activity failure。
- gzip 自动协商与解压、redirect、framing 等由 `net/http` 在 HTTP 边界处理；Response 表达的是 `net/http` 暴露的 HTTP 对象，不承诺与 wire bytes 相同。
- 后端返回的任何 HTTP status（包括 4xx/5xx）都作为正常 `Response` 返回，保留后端语义，不自动变成 Activity failure。
- 仅当最终 HTTP status 在 action 的 `response_schemas` 中存在精确匹配，且 response 是 JSON 时才校验；没有配置的 status（包括 202）不校验。首版不支持 `2xx`、范围或 `default` 通配规则。
- DNS、连接、超时等未获得 HTTP response 的错误才作为 Activity failure；是否重试由调用 Workflow 的 Activity Retry Policy 决定。
- schema 不匹配和服务已删除返回 non-retryable Application Error。
- 取消采用协作式语义：持续执行的 HTTP Activity 周期性发送空 heartbeat，使未启用 proactive Worker Commands 的 Temporal Server 也能下发取消；收到的 Go context cancellation 会取消正在进行的 HTTP request。调用 Workflow 通过 `HeartbeatTimeout` 控制取消传播延迟，并通过 `WaitForCancellation` 决定是否等待 Activity 实际退出。HTTP 连接取消只终止 Gateway 本地调用；根据当前系统约束，可安全假设远端不会在断开后留下副作用。

Activity 参数仍受 Temporal payload/Event History 限制；大对象必须由调用方改传对象存储引用，不在首版 Gateway 中增加对象存储协议。

输入输出暂不预留 `Extra`。默认 JSON Data Converter 对新增可选 struct 字段是向后兼容的，应在出现明确语义时增加命名字段，而不是提前形成无约束协议。若将来确实需要 Gateway 不解释的扩展元数据，使用 `map[string]json.RawMessage` 并单独规定透传、大小和敏感数据规则，不使用 `map[string]any`。

### 5. SQLite 是配置源，Temporal 是路由源

SQLite 保存完整历史，不覆盖旧 revision：

- `services`：`id`、`state`、`current_revision`、时间戳。
- `service_revisions`：`service_id`、`revision`、`kind`（`service`/`tombstone`）、`state`、`switch_started`、规范化 `spec_json`、配置 hash、时间戳。

Activity Type、Task Queue、Deployment Name 和 Build ID 均由 `service_id/revision` 推导，不重复存储。

相同规范化配置 hash 的 `PUT` 是 no-op，不创建 revision。SQLite 保存配置和期望状态；Temporal Worker Deployment 的 Current Version 决定实际新任务路由。启动时 Reconciler 比对两边并修复中断的切换。

首版只允许一个 Gateway 进程管理同一个 SQLite 文件和 Temporal Namespace，不实现多控制器选主。

### 6. REST 管理面

- `PUT /services/{id}`：新增或更新；成功响应前必须完成 Worker 就绪和 Current Version 切换。
- `GET /services`：列出服务。
- `GET /services/{id}`：读取当前配置和 revision 状态。
- `DELETE /services/{id}`：热删除。
- `GET /healthz`：检查进程、SQLite 和 Temporal 连接。

管理 API 与 Activity Task Queue 是两个边界：REST API 只管理配置，不代理业务调用。

### 7. 删除使用 tombstone version

删除服务时创建一个新的 tombstone revision，注册同名 Activity，但固定返回 non-retryable `ServiceDeleted`：

1. 持久化 tombstone revision。
2. 启动 tombstone Worker 并设为 Current Version。
3. 将 service 标记为 `deleted`，阻止普通更新之外的管理操作。
4. 旧 revision 排空后停止。

tombstone Worker 必须保留，才能让删除后仍错误调度到稳定 Task Queue 的新任务快速失败，而不是永远等待 Worker。重新 `PUT` 同一 `service_id` 时可创建更高 revision 并恢复服务。

删除切流时，旧 Worker 中已经开始的 Activity 允许完成。按上述已验证的 Current Version 分发规则，尚未开始的 backlog 会跟随 Current Version 进入 tombstone 并返回 `ServiceDeleted`，包括删除前已经入队的任务。

### 8. WCI 暂不启用，但保留兼容映射

本地 Gateway Worker 始终主动 poll，因此不配置 compute provider，也不启用 WCI。Runpod 和阿里云 FC 是 Activity 调用目标，不是 Temporal 启动 Worker 的 compute provider。

未来若把 Gateway Worker 本身部署到 Temporal 官方支持的 Serverless Worker provider，可沿用 `{service_id}` Deployment 和 `rN` Build ID，仅为 Worker Deployment Version 增加 compute config；这不改变 Activity 契约和 Registry 模型。

## 进程结构

首版为一个 Go binary，包含：

```text
HTTP Control API
       │
       ▼
SQLite Registry ◄──► Reconciler / Worker Manager
                           │
                           ├── service-a Worker r3 (current)
                           ├── service-a Worker r2 (draining)
                           └── service-b Worker r7 (current)
                                      │
                                      ▼
                                  HTTP backend
```

所有 Worker 共享一个 Temporal client 和一个 HTTP client；每个 revision 只有 Worker、注册的 Activity closure 和本地 in-flight 计数是独立的。

同一进程内的管理面变更由 Manager 串行执行，确保同一服务的 PUT、DELETE 和 revision 切换具有全序；Activity 执行不经过该锁，可由 Temporal Worker 并发调度。首版的全局管理锁也会串行不同服务的更新；等待时间包括前序请求排队、切流和旧 Worker 排空，但不会阻塞已经运行的 Activity。关闭时所有 Worker 同时开始 graceful Stop，避免按服务数串行累加排空时间。

## 安全与运行约束

- 管理 API 可以注册任意 endpoint，等同于受控 SSRF/出网能力。首版本地默认只监听 loopback；生产部署前必须增加管理面鉴权和网络层 endpoint/egress 约束。
- 透传的鉴权 header 和 body 会进入 Temporal Event History。真实凭据投入生产前必须配置 Payload Codec 加密，或改为传 secret reference；日志不得输出 header/body。
- HTTP Activity 不能在一次请求中可靠 checkpoint。长请求必须设置合理的 Start-To-Close Timeout，并依赖后端幂等键承受 Temporal retry。
- HTTP response 必须完整读入内存后才能成为 Temporal payload，因此 Gateway 默认将响应体限制为 1 MiB，并允许通过进程启动参数调整；更大的结果优先改传对象存储引用。
- 当前本地 Temporal 使用 in-memory persistence；Temporal Server 重启会丢失 History、Task 和 Worker Deployment 状态。SQLite 只能恢复服务配置，不能恢复这些 Temporal 数据。

## 被否决的方案

### Dynamic Activity

单个动态 Activity 可以减少 Worker 数量，但 Temporal UI 和调用契约只看到一个通用 Activity，不符合“一个实际后端服务一个 Activity”的要求。

### 每个 revision 使用独立 Task Queue

`{service_id}/rN` 可以手工实现版本路由，但需要额外的 `resolve-service` Activity，仍存在解析与调度之间的切换竞态，并重复实现 Temporal 已有的 Current Version、回滚和 drainage 语义。

### 直接使用 Temporal Serverless Workers/WCI 调用后端

WCI 启动的是连接回 Temporal 并 poll Task Queue 的 Worker，不是通用 webhook dispatcher；官方 compute provider 配置也不包含 Runpod 或阿里云 FC。强行扩展 Server 侧 provider 会增加维护面，且没有必要。

### provider SDK adapter

所有目标均能通过 HTTP 调用，provider SDK 不会改善 HTTP 对象语义透明性，反而增加依赖和各平台语义分支。

## 影响

正面影响：

- Activity 名称和 Task Queue 对调用方稳定，revision 切换由 Temporal 原生路由完成。
- 新版本未就绪时无法成为 Current，失败更新不会影响当前服务。
- 删除后的错误调用有确定、快速、可观测的失败结果。
- 后端平台差异不进入核心调用路径。

代价：

- 每个 active/draining/tombstone revision 都有一个本地 Worker 实例。
- 不同服务的控制面变更目前共享全局锁，慢切流会增加其他服务配置更新的延迟；业务 Activity 调度不受影响。
- Worker Deployment 管理 API 在 Go SDK 中仍标记为 experimental，升级 SDK/Server 时需要做兼容验证。
- tombstone Worker 不能自动物理删除；只有确认所有调用方都不再调度该服务后才能人工清理。
- HTTP 对象语义透明不等于不落盘，Temporal History 的体积和敏感数据治理仍由部署方负责。

## 参考

- [Temporal Serverless Workers on AWS Lambda](https://docs.temporal.io/serverless-workers/aws-lambda)
- [Temporal Worker Versioning](https://docs.temporal.io/worker-versioning)
- [Temporal Task Queue performance and enhanced describe](https://docs.temporal.io/develop/worker-performance/task-queues)
- [Temporal Server 1.32 release](https://github.com/temporalio/temporal/releases/tag/v1.32.0)
- [Temporal auto-scaled workers / WCI](https://github.com/temporalio/temporal-auto-scaled-workers)
- [Temporal Go SDK client API](https://pkg.go.dev/go.temporal.io/sdk/client)
