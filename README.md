# temporaless

`temporaless` 是一个 Go 编写的 Temporal Activity Worker Gateway。每个注册的 HTTP 服务拥有稳定的 Activity Type、Task Queue 和 Worker Deployment；配置 revision 通过 Build ID 完成热新增、热更新和 tombstone 热删除。

```text
Workflow ── Temporal Activity ── temporaless ── HTTP backend
                 │                    │
       stable service ID       hot revision switch
```

Gateway 不引入 Runpod、阿里云 FC 等 provider SDK。只要后端提供 HTTP endpoint，就可以注册为 Temporal Activity。

## Quick Start

需要 Go 1.25.4+ 和 Temporal CLI。以下命令分别在不同终端运行。

启动本地 Temporal：

```bash
temporal server start-dev
```

启动仓库自带的 HTTP 后端：

```bash
go run ./examples/backend
```

启动 Gateway：

```bash
go run ./cmd/temporaless \
  -config temporaless.example.yaml
```

注册服务：

```bash
curl --fail-with-body \
  -X PUT http://127.0.0.1:8080/services/demo-service \
  -H 'Content-Type: application/json' \
  -d '{
    "actions": {
      "invoke": {
        "method": "POST",
        "endpoint": "http://127.0.0.1:18080/runsync"
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

运行真实的 Workflow Worker 和 Temporal Client：

```bash
go run ./examples/caller
```

预期结果：Workflow 将 `demo-service/invoke` 调度到 `demo-service` Task Queue，Gateway 调用示例后端并返回 `200 OK` 和 JSON body。Temporal UI 位于 <http://localhost:8233>。

逐步说明、完整 schema 和预期输出见 [Quick Start](docs/quickstart.md)。

## 稳定且语言无关的 Temporal 契约

调用方始终使用：

- Activity Type：`{service_id}/invoke`
- Task Queue：`{service_id}`

调用方只依赖所用语言的官方 Temporal SDK。注册和管理服务使用普通 HTTP，不需要导入 `temporaless` Go module 或任何专用客户端。

Activity 参数是一个 JSON 对象。`body` 是原始 HTTP body 的 Base64：

```json
{
  "action": "invoke",
  "headers": {
    "Authorization": ["Bearer ..."],
    "Content-Type": ["application/json"],
    "Idempotency-Key": ["op-123"]
  },
  "body": "eyJwcm9tcHQiOiJoZWxsbyJ9"
}
```

Go 示例在调用方本地声明等价 struct，其他语言直接构造同一 JSON 对象。响应包含 `status`、`message`、`headers` 和 Base64 `body`。Gateway 保持 HTTP 对象语义，不映射业务对象；redirect、gzip 和 framing 仍按 Go `net/http` 行为处理。

HTTP 4xx/5xx 是正常 Activity 结果。DNS、连接错误和 timeout 是 Activity failure，由调用方 Retry Policy 决定是否重试。Gateway 发送 Activity heartbeat，使 Workflow cancellation 能取消正在进行的 HTTP request。

完整调用代码见 [examples/caller/main.go](examples/caller/main.go)。

## 管理 API

| Method | Path | 作用 |
|---|---|---|
| `PUT` | `/services/{id}` | 新增、更新或恢复服务，成功前完成 Worker 切流 |
| `GET` | `/services` | 列出全部服务和 revision 状态 |
| `GET` | `/services/{id}` | 读取单个服务 |
| `GET` | `/services/{id}/health` | 调用可选的后端 health endpoint 并记录 latency |
| `DELETE` | `/services/{id}` | 切换到 tombstone revision |
| `GET` | `/healthz` | 检查 SQLite 和 Temporal 连接 |
| `GET` | `/openapi.yaml` | OpenAPI 3.1 规范 |
| `GET` | `/docs` | Swagger UI |

完整请求、响应、状态码和配置字段见 [REST 与 Temporal API](docs/api.md)。

## 热更新和删除

相同规范化配置的重复 `PUT` 是 no-op。配置变化会：

1. 在 SQLite 创建 starting revision。
2. 启动新 Build ID Worker。
3. 将 Worker Deployment Current Version 切到新 revision。
4. 激活新 revision 并优雅排空旧 Worker。

`DELETE` 创建 tombstone revision。之后错误调度到稳定 Task Queue 的新 Activity 会快速得到 non-retryable `ServiceDeleted`，不会永久等待没有 Worker 的队列。重新 `PUT` 相同 service ID 可以恢复服务。

## YAML 配置

Gateway 只保留一个启动参数：

```bash
go run ./cmd/temporaless -config temporaless.example.yaml
```

省略 `-config` 时使用内建默认值。完整 YAML 和每个参数的默认值、单位与作用见 [Gateway YAML 配置](docs/api.md#gateway-yaml-配置)。

## 可观测性

进程日志统一输出 JSON 到 stdout，Temporal Go SDK 日志也通过同一 logger 输出，并带有 `service=temporaless` 和 `component=temporal_sdk` 字段。由 vlagent、Vector 或容器日志采集器转发到 VictoriaLogs 即可，Gateway 不持有 VictoriaLogs 地址或凭据。

Prometheus metrics server 默认关闭。配置 `observability.metrics.listen` 后，它会在独立端口只提供 `GET /metrics`，包含 Temporal SDK、Go runtime 和进程指标；可由 vmagent 或其他 Prometheus-compatible scraper 采集。例如仓库示例配置启用：

```bash
curl http://127.0.0.1:9090/metrics
```

该端点没有鉴权，不应直接暴露到公网。

## Docker

CI 会将 Linux amd64 镜像发布到 GHCR。默认分支使用 `latest`，分支、Git tag、SemVer 和 commit 另有独立 tag：

```bash
docker pull ghcr.io/qiyin-tech/temporaless:latest
```

仓库关联的 GitHub Actions 使用 `GITHUB_TOKEN` 发布，无需额外 registry secret；部署侧使用组织级 GHCR 权限拉取私有镜像。本地构建命令为：

```bash
docker buildx build --platform linux/amd64 --load -t temporaless:dev .
```

容器不内置环境配置。复制 `temporaless.example.yaml` 为 `temporaless.docker.yaml`，至少修改以下字段：

```yaml
server:
  listen: 0.0.0.0:8080
registry:
  path: /var/lib/temporaless/temporaless.db
temporal:
  address: host.docker.internal:7233
observability:
  metrics:
    listen: 0.0.0.0:9090
```

其余字段继续沿用示例值，然后将 YAML 只读挂载到固定路径，将 SQLite 目录挂到持久卷：

```bash
docker run --rm --name temporaless \
  --add-host host.docker.internal:host-gateway \
  -p 8080:8080 \
  -p 9090:9090 \
  --mount type=bind,src="$PWD/temporaless.docker.yaml",dst=/etc/temporaless/config.yaml,readonly \
  --mount type=volume,src=temporaless-data,dst=/var/lib/temporaless \
  ghcr.io/qiyin-tech/temporaless:latest
```

`host.docker.internal` 仅用于本机 Docker；部署时应改成容器网络可达的 Temporal Frontend 地址。配置文件必须对容器 UID `65532` 可读；数据卷由镜像预设为该 UID 可写。配置不通过 build argument、环境变量或镜像层注入，避免环境信息和未来可能出现的凭据进入镜像历史。

## 验证

```bash
go test ./...
TEMPORAL_SMOKE=1 go test ./...
TEMPORALESS_STRESS=1 go test -run '^TestSustainedControlPlaneStress$' -timeout 1m ./internal/control
```

第二条命令需要 `localhost:7233` 上运行 Temporal，覆盖 Worker Deployment backlog、并发 in-flight Activity 排空、heartbeat 取消传播以及完整的 r1 → r2 → tombstone 调用链。

第三条命令运行 30 秒可选压力测试，不连接 Temporal；它持续并发执行 REST PUT/DELETE/GET/LIST/healthz，并校验 SQLite revision、runtime current 和存活 Worker 最终一致。默认 `go test ./...` 会跳过它。

## 文档

- [Quick Start](docs/quickstart.md)：从本地环境到真实 Workflow 调用。
- [REST 与 Temporal API](docs/api.md)：配置 schema、管理 API、Activity contract、错误和参数。
- [ADR-0001](docs/adr/0001-hot-reloadable-http-activity-workers.md)：版本路由、失败恢复、取消和 tombstone 的设计依据。

## Limitations

- 管理 API 没有内建鉴权，并允许注册任意 HTTP/HTTPS endpoint。默认 loopback 只适合本地使用；生产环境必须增加入口鉴权和 endpoint/egress 网络约束。
- Activity header、请求体和响应体会进入 Temporal Event History。生产凭据必须配置 Payload Codec 加密，或改传 secret reference。
- 后端响应体默认限制为 1 MiB，可通过启动参数调整。大对象更适合返回对象存储引用。
- 首版只允许一个 Gateway 进程管理同一个 SQLite 文件和 Temporal Namespace。
