# temporaless 远端负载测试（2026-11-02）

本报告记录 `8dea5ca` 在 Linux amd64 测试机上的一次端到端负载测试。测试通过真实 Temporal Workflow 调度 temporaless Activity，再由 temporaless 调用立即返回 `204 No Content` 的 HTTP 后端。

结论：该环境中 50 TPS 可以稳定完成，temporaless 平均只使用约 `0.103` 个 CPU core，RSS 峰值为 `48.60 MiB`。目标速率达到 100 TPS 后，端到端延迟急剧增长，但 temporaless 的 CPU 和内存没有接近饱和，瓶颈先出现在同机 Temporal dev 和 Workflow 调度链路。因此 50 TPS 是本次环境的稳定点，不是 temporaless 的吞吐上限。

## 测试范围

只报告 temporaless 进程的 CPU、RSS、Go heap 和 goroutine。Temporal Server、负载生成器和模拟 HTTP 后端只用于产生负载，其资源使用不计入结果。

测试机：

| 项目 | 值 |
|---|---|
| 主机 | `10.0.0.40` |
| OS | Ubuntu 24.04 LTS |
| CPU | 4 vCPU，x86_64 |
| 内存 | 8 GiB |
| Docker | 29.9.2 |
| temporaless | commit `8dea5ca`，Linux amd64 容器 |
| Temporal | CLI image 1.9.1，Server 1.32.0，in-memory dev server |

temporaless 使用以下关键配置：

```yaml
temporal:
  address: 127.0.0.1:7233
  namespace: default

worker:
  max_concurrent_activities_per_service: 32

http:
  max_backend_response_bytes: 1048576
  max_idle_connections_per_host: 32

observability:
  metrics:
    listen: 127.0.0.1:19090
```

## 负载模型

- 每个 Workflow 只执行一次 `bench-service/invoke` Activity。
- Activity Task Queue 为 `bench-service`，后端为本机 HTTP endpoint，立即返回 `204 No Content`。
- 每档按固定速率启动 Workflow，持续 20 秒。
- Activity 禁用 retry，`ScheduleToCloseTimeout` 为 30 秒，`StartToCloseTimeout` 为 10 秒，`HeartbeatTimeout` 为 2 秒。
- 每 250 ms 从 temporaless `/metrics` 采样一次。
- 25、50、100、200 TPS 顺序执行；400 TPS 使用全新的 Temporal、SQLite 和 temporaless 实例单独复测，避免上一档超时后仍在运行的 Workflow 污染结果。

指标定义：

- `稳态完成 TPS`：20 秒发压窗口内完成的 Workflow 数除以 20 秒。
- `完成 / 等待超时`：发压窗口结束后继续等待最多 45 秒的结果；等待超时不等同于 temporaless Activity failure。
- latency：从启动 Workflow 到获得结果的端到端时间，只统计成功完成项。
- CPU：根据 `process_cpu_seconds_total` 的增量计算，`100%` 表示占满一个 CPU core；在本次 4 vCPU 主机上，表中数值除以 4 才是整机 CPU 百分比。
- RSS：`process_resident_memory_bytes`。
- Heap：`go_memstats_heap_alloc_bytes`。

## 结果

temporaless 启动后的空闲基线为：RSS `26.62 MiB`、Heap `3.78 MiB`、17 goroutine。

| 目标 TPS | 稳态完成 TPS | 完成 / 等待超时 | P50 | P95 | P99 | CPU 平均 / 峰值 | RSS 峰值 | Heap 峰值 | goroutine 峰值 |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 25 | 24.85 | 500 / 0 | 146.9 ms | 170.1 ms | 176.4 ms | 6.80% / 16.11% | 48.60 MiB | 17.39 MiB | 33 |
| 50 | 49.50 | 1000 / 0 | 189.7 ms | 340.4 ms | 468.1 ms | 10.28% / 17.42% | 48.60 MiB | 17.96 MiB | 33 |
| 100 | 43.30 | 2000 / 0 | 8.52 s | 22.51 s | 24.87 s | 9.40% / 16.45% | 49.47 MiB | 19.57 MiB | 36 |
| 200 | 7.35 | 1123 / 2877 | 23.11 s | 35.71 s | 41.01 s | 3.44% / 11.92% | 49.29 MiB | 18.87 MiB | 32 |
| 400（隔离复测） | 0.30 | 8 / 7992 | 2.33 s | 63.57 s | 63.57 s | 2.30% / 12.61% | 42.74 MiB | 13.93 MiB | 32 |

100 TPS 档虽然最终完成 2000 个 Workflow，但发压结束后又排空了 25.61 秒，因此不能视为持续承载 100 TPS。200 和 400 TPS 档的 latency 分位数只来自少量成功项，不能代表全部请求。

负载生成器的原始汇总如下。`errors` 表示等待 deadline 到期，不表示 temporaless 进程错误：

```csv
target_tps,offered,steady_completed,steady_tps,total_completed,errors,p50_ms,p95_ms,p99_ms,drain_ms,cpu_avg_pct,cpu_peak_pct,rss_peak_mib,heap_peak_mib,goroutines_peak
25,500,497,24.85,500,0,146.893,170.088,176.374,81.327,6.80,16.11,48.60,17.39,33
50,1000,990,49.50,1000,0,189.697,340.421,468.080,166.021,10.28,17.42,48.60,17.96,33
100,2000,866,43.30,2000,0,8521.626,22509.914,24871.070,25611.387,9.40,16.45,49.47,19.57,36
200,4000,147,7.35,1123,2877,23108.603,35709.294,41012.684,30142.341,3.44,11.92,49.29,18.87,32
400,8000,6,0.30,8,7992,2329.231,63572.762,63572.762,45021.892,2.30,12.61,42.74,13.93,32
```

## 判断

### 稳定负载下的 temporaless 成本

50 TPS 时，temporaless 平均 CPU 为单核的 `10.28%`，即约 `0.103 core`，约占整台 4 vCPU 主机的 `2.57%`。按完成吞吐折算，每次调用约消耗 `2.1 ms` temporaless CPU time。

从 25 TPS 增加到 50 TPS，RSS 峰值保持 `48.60 MiB`，Heap 只增加 `0.57 MiB`。这次短时测试没有观察到内存随 TPS 线性增长。

### 高目标速率没有压满 temporaless

目标速率从 50 TPS 提高到 100 TPS 后，P95 从 `340 ms` 增长到 `22.51 s`，但 temporaless 平均 CPU 从 `10.28%` 降到 `9.40%`，RSS 仍约 `49 MiB`。200 和 400 TPS 时 CPU 进一步下降。

这说明高目标速率下，Activity 没有被足够快地投递给 temporaless。当前数据只能确定同机 Temporal dev、Workflow Worker 和 workflow-per-request 负载模型组成的链路在约 50 TPS 后开始排队，不能据此确定 temporaless Worker 自身的最大 TPS。

## 使用这些数据

- 可把 `50 TPS / 0.103 core / 48.60 MiB RSS` 作为当前版本在该测试条件下的已验证基线。
- 不应把 50 TPS 写成生产容量上限，也不应按上述两个稳定档位直接线性外推。
- 若要测 temporaless 的饱和点，应使用独立 Temporal 集群和足以持续填满 Activity Task Queue 的负载生成方式，并分别观测 Temporal backlog 与 temporaless Activity slot 使用率。
- 生产 sizing 还需要覆盖真实后端延迟、不同 payload 大小、多服务竞争和持续运行时的内存稳定性。

本次负载生成器是临时测试工具，没有提交到仓库。报告保留了复测所需的负载语义和配置，但目前不能通过仓库中的单条命令复现。
