# Async Task Spring Boot Starter

面向 Spring Boot 4 的可靠异步任务 starter，使用 transactional outbox 和 Kafka 提供跨进程异步任务。
当前支持 Java 21、PostgreSQL 13+ 与 MySQL 8.0+。

## 交付语义

本项目提供 **at-least-once** 投递，不承诺 exactly-once。Kafka 已确认发送但 outbox 状态回写失败时，
同一信封可能再次投递。等待 Kafka 确认超时或投递租约失效时，starter 会将其视为投递结果未知，
即使已经达到普通最大尝试次数也会继续重投。调用方必须提供稳定幂等键，业务 `AsyncTaskHandler`
也必须保证幂等。

生产侧和消费侧共享同一张 outbox 表：

- `async_task_outbox` 与业务数据在同一个本地事务内写入。
- 同一条 outbox 记录继续保存投递状态、执行租约、消息重投和最终状态。
- `DISPATCHING` 与 `RUNNING` 租约超时后都可以被其他实例接管。

因此，生产任务和消费任务的应用必须访问同一个业务数据库和同一张 `async_task_outbox` 表。

## 引入依赖

```xml
<dependency>
    <groupId>com.zaviwayne</groupId>
    <artifactId>async-task-spring-boot-starter</artifactId>
    <version>1.0.0</version>
</dependency>
```

应用还需自行引入 PostgreSQL 或 MySQL JDBC 驱动。starter 不传递数据库驱动。

## 数据库

生产环境建议使用 Flyway 或 Liquibase 管理模块内的脚本：

- PostgreSQL：`classpath:db/async-task-postgresql-schema.sql`
- MySQL：`classpath:db/async-task-mysql-schema.sql`

两份脚本均不包含 `DROP TABLE`。开发环境可以显式启用内置初始化：

```yaml
async-task:
  database:
    platform: auto
    initialize-schema: true
```

MySQL 必须使用 UTC 会话时区，推荐连接串包含：

```text
connectionTimeZone=UTC&forceConnectionTimeZoneToSession=true
```

## Kafka 配置

```yaml
spring:
  kafka:
    bootstrap-servers: localhost:9092
    producer:
      acks: all
      transaction-id-prefix: ${spring.application.name}-${HOSTNAME:local}-
      properties:
        enable.idempotence: true
    consumer:
      auto-offset-reset: earliest
      properties:
        isolation.level: read_committed

async-task:
  kafka:
    auto-create-topics: true
    transaction-enabled: true
    consumer-group: resume-worker
    concurrency: 2
    partitions: 3
    replicas: 1
    max-retries: 3
    retry-interval: 2s
    dead-letter-suffix: .DLT
    dead-letter-max-retries: 10
    parking-suffix: .PARKING
    bindings:
      - topic: resume-task
        dead-letter-topic: resume-task-dead
        parking-topic: resume-task-parking
        consumer-group: resume-parse-worker
        concurrency: 3
      - topic: job-task
```

Kafka key 使用稳定幂等键，消息 value 使用 JSON 字符串。消费端会校验业务 Topic 与信封 `destination`、
Kafka key 与信封 `idempotencyKey` 一致。业务主题、对应 DLT 和 parking 主题应使用相同分区数。
`bindings` 中留空的配置使用 Kafka 全局默认值；原有 `topics` 字符串列表仍然兼容，也可以由同名
`binding` 逐步覆盖。生产环境必须保证每个应用实例的 `transaction-id-prefix` 唯一。
信封格式、Topic/Key 身份、幂等冲突、缺少 Handler 和业务载荷反序列化错误属于永久异常，
不会执行无意义的业务重试。DLT 处理失败达到上限后，消息会进入 parking 主题。starter 不为 parking
主题启动消费者，由运维或业务管理流程人工处置。

## 提交任务

`enqueue` 强制要求已有数据库事务，避免业务数据已经提交但 outbox 消息缺失：

```java
@Transactional(rollbackFor = Exception.class)
public void createResume(Resume resume) {
    resumeRepository.save(resume);
    asyncTaskEnqueuer.enqueue(AsyncTaskRequest.of(
        "resume-task",
        "resume.parse",
        1,
        new ResumeParsePayload(resume.getId()),
        "resume.parse:" + resume.getId()));
}
```

相同幂等键和相同任务内容会返回第一次创建的任务 ID；相同幂等键对应不同内容时会抛出
`DuplicateAsyncTaskException`。

需要按业务对象检索任务时，可以同时写入通用关联类型与关联标识：

```java
asyncTaskEnqueuer.enqueue(AsyncTaskRequest.referenced(
    "resume-task",
    "resume.parse",
    1,
    new ResumeParsePayload(resume.getId()),
    "resume.parse:" + resume.getId(),
    "resume",
    resume.getId().toString()));
```

`referenceType` 与 `referenceId` 必须同时填写或同时留空；数据库为这两个字段建立了联合索引。

## 查询与重新入队

业务系统通过 `AsyncTaskAdmin` 查询任务，不需要依赖 JDBC 状态存储：

```java
Optional<AsyncTaskInfo> task = asyncTaskAdmin.findByTaskId(taskId);
Optional<AsyncTaskInfo> idempotentTask = asyncTaskAdmin.findByIdempotencyKey(idempotencyKey);
List<AsyncTaskInfo> resumeTasks = asyncTaskAdmin.findByReference("resume", resumeId, 20, 0);
```

查询结果包含当前状态、进度 JSON、错误摘要、投递和执行次数以及各生命周期时间。
`requeue(taskId)` 只接受 `SUCCESS` 或 `DEAD` 终态任务，并返回明确的 `REQUEUED`、`NOT_FOUND`
或 `NOT_TERMINAL` 结果。重新入队会递增消息 `generation`，清理上一轮的租约、错误、进度和尝试次数，
再从 `PENDING` 开始投递；上一代延迟到达的业务消息或 DLT 不会改变新一代任务状态。

## 处理任务

应用通过 Spring Bean 注册处理器：

```java
@Component
public final class ResumeParseHandler implements AsyncTaskHandler<ResumeParsePayload> {
    @Override
    public String taskType() {
        return "resume.parse";
    }

    @Override
    public int schemaVersion() {
        return 1;
    }

    @Override
    public Class<ResumeParsePayload> payloadType() {
        return ResumeParsePayload.class;
    }

    @Override
    public void handle(AsyncTaskContext context, ResumeParsePayload payload) {
        // 业务写入需要以幂等键或业务唯一约束保证重复执行安全。
        context.updateProgress(new ParseProgress(10, 100));
    }
}
```

starter 会在 Handler 执行期间自动刷新执行租约；业务也可以调用 `context.heartbeat()` 立即刷新。
`context.updateProgress(...)` 会把对象序列化到 `progress_json`，同时刷新并校验当前执行租约。
业务处理时间仍应小于 Kafka `max.poll.interval.ms`，或相应增大该配置。

## 可观测性

存在 Micrometer `MeterRegistry` 时，starter 自动注册以下指标：

- `async.task.enqueued`：成功入队数量。
- `async.task.dispatch`：投递成功、重试和死信数量。
- `async.task.execution`：执行成功和失败数量。
- `async.task.dead.letter`：进入 DLT 终态数量。
- `async.task.lease.recovered`：投递或执行租约接管数量。
- `async.task.requeued`、`async.task.cleaned`：重新入队和清理数量。
- `async.task.backlog`、`async.task.running`、`async.task.dead`：当前队列状态。
- `async.task.oldest.backlog.age`：最老积压任务的存续秒数。

应用引入 Spring Boot Actuator 后还会注册 `asyncTaskHealthIndicator`，只检查任务表是否可查询；任务数量和
最老积压时间通过上述 Micrometer 指标提供。Actuator 端点是否暴露仍由业务应用的 management 配置控制。

## 终态数据清理

自动清理默认关闭。启用后，每轮按批删除超过保留时间的 `SUCCESS` 和 `DEAD` 记录，
某批未删满时提前结束，并由最大批数限制单轮负载：

```yaml
async-task:
  retention:
    enabled: true
    retention-period: 720h
    cleanup-interval: 1h
    batch-size: 500
    max-batches-per-run: 200
```

## 主要配置

| 配置项 | 默认值 | 含义 |
| --- | --- | --- |
| `async-task.enabled` | `true` | 是否启用 starter 基础能力 |
| `async-task.database.platform` | `auto` | `auto`、`postgresql` 或 `mysql` |
| `async-task.database.initialize-schema` | `false` | 是否执行内置建表脚本 |
| `async-task.outbox.enabled` | `true` | 是否启动 Outbox 定时投递作业 |
| `async-task.outbox.poll-interval` | `1s` | Outbox 轮询间隔，至少为 `1ms` |
| `async-task.outbox.batch-size` | `20` | 单次抢占数量 |
| `async-task.outbox.lease-duration` | `5m` | 投递租约时长，必须大于 `batch-size × send-timeout` |
| `async-task.outbox.execution-lease-duration` | `5m` | 业务执行租约时长 |
| `async-task.outbox.execution-heartbeat-interval` | `30s` | 自动刷新执行租约的间隔 |
| `async-task.outbox.execution-heartbeat-threads` | `2` | 自动刷新执行租约的调度线程数 |
| `async-task.outbox.max-attempts` | `8` | 明确投递失败的最大尝试次数；投递结果未知时不受此限制 |
| `async-task.outbox.initial-backoff` | `2s` | 首次投递失败后的退避时间 |
| `async-task.outbox.max-backoff` | `1h` | 投递指数退避时间上限 |
| `async-task.outbox.max-envelope-bytes` | `1000000` | 任务信封 JSON 最大 UTF-8 字节数 |
| `async-task.outbox.max-progress-bytes` | `1000000` | 任务进度 JSON 最大 UTF-8 字节数 |
| `async-task.retention.enabled` | `false` | 是否自动清理成功和死信终态任务 |
| `async-task.retention.retention-period` | `720h` | 终态任务保留时间 |
| `async-task.retention.cleanup-interval` | `1h` | 自动清理间隔，至少为 `1ms` |
| `async-task.retention.batch-size` | `500` | 单批清理数量 |
| `async-task.retention.max-batches-per-run` | `200` | 单次运行最大清理批数 |
| `async-task.observability.statistics-cache-duration` | `30s` | Micrometer 任务运行统计缓存时长 |
| `async-task.kafka.enabled` | `true` | 是否启用 Kafka 投递和消费能力 |
| `async-task.kafka.auto-create-topics` | `false` | 是否自动声明业务、DLT 和 parking 主题 |
| `async-task.kafka.transaction-enabled` | `false` | 是否强制使用 Kafka 事务发送和 `read_committed` 消费 |
| `async-task.kafka.topics` | `[]` | 使用全局默认值的业务 Topic 列表，保留用于兼容旧配置 |
| `async-task.kafka.bindings` | `[]` | 每个业务 Topic 的独立配置列表 |
| `async-task.kafka.bindings[].topic` | 无 | 业务 Topic，配置 binding 时必填且不能重复 |
| `async-task.kafka.bindings[].dead-letter-topic` | `<topic><dead-letter-suffix>` | 对应 DLT Topic，可覆盖默认后缀规则 |
| `async-task.kafka.bindings[].parking-topic` | `<dead-letter-topic><parking-suffix>` | DLT 处理耗尽后的停放主题 |
| `async-task.kafka.bindings[].consumer-group` | 全局值 | 当前 Topic 的消费组 |
| `async-task.kafka.bindings[].concurrency` | 全局值 | 当前 Topic 的消费并发数 |
| `async-task.kafka.bindings[].partitions` | 全局值 | 当前 Topic 自动声明时的分区数 |
| `async-task.kafka.bindings[].replicas` | 全局值 | 当前 Topic 自动声明时的副本数 |
| `async-task.kafka.consumer-group` | `async-task` | Topic 未覆盖时使用的默认消费组 |
| `async-task.kafka.concurrency` | `1` | Topic 未覆盖时使用的默认消费并发数 |
| `async-task.kafka.partitions` | `3` | 自动声明 Topic 时的默认分区数 |
| `async-task.kafka.replicas` | `1` | 自动声明 Topic 时的默认副本数 |
| `async-task.kafka.send-timeout` | `10s` | 等待 Kafka 确认的超时时间，至少为 `1ms` |
| `async-task.kafka.retry-interval` | `2s` | 业务消息及 DLT 处理失败后的重试间隔，至少为 `1ms` |
| `async-task.kafka.max-retries` | `3` | 业务消息进入 DLT 前的重试次数 |
| `async-task.kafka.lease-retry-interval` | `5s` | 执行租约仍有效时的重试间隔，至少为 `1ms` |
| `async-task.kafka.dead-letter-suffix` | `.DLT` | 未单独配置 DLT Topic 时使用的默认后缀 |
| `async-task.kafka.dead-letter-max-retries` | `10` | DLT 处理失败进入 parking 主题前的重试次数 |
| `async-task.kafka.parking-suffix` | `.PARKING` | 未单独配置 parking 主题时使用的默认后缀 |

## 构建

```bash
./mvnw -U clean verify
```

仓库不内置 Maven 镜像和认证配置。依赖仓库镜像、代理及网络超时参数统一在本机 Maven
`~/.m2/settings.xml` 中管理。

## 版本发布

维护者发布新版本到 Maven Central 的流程见 [RELEASING.md](RELEASING.md)。

## License

Apache License 2.0
