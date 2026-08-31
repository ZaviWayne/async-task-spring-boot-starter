package com.zaviwayne.asynctask.jdbc;

import com.zaviwayne.asynctask.core.*;

import java.time.Clock;
import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/**
 * 基于 outbox 执行租约的异步任务处理器。
 *
 * @since 2026-08-27
 */
public final class AsyncTaskProcessor implements AutoCloseable {
    /**
     * 自动心跳线程名称前缀。
     */
    private static final String HEARTBEAT_THREAD_NAME_PREFIX = "async-task-heartbeat-";

    /**
     * 默认自动心跳线程数。
     */
    private static final int DEFAULT_HEARTBEAT_THREADS = 2;

    /**
     * JDBC 状态存储。
     */
    private final JdbcTaskStore taskStore;

    /**
     * 处理器注册表。
     */
    private final AsyncTaskHandlerRegistry handlerRegistry;

    /**
     * 载荷序列化器。
     */
    private final TaskPayloadSerializer serializer;

    /**
     * 系统时钟。
     */
    private final Clock clock;

    /**
     * 执行租约时长。
     */
    private final Duration leaseDuration;

    /**
     * 自动心跳间隔。
     */
    private final Duration heartbeatInterval;

    /**
     * 自动心跳调度器。
     */
    private final ScheduledExecutorService heartbeatExecutor;

    /**
     * 任务生命周期观测器。
     */
    private final AsyncTaskObserver observer;

    /**
     * 创建异步任务处理器。
     *
     * @param taskStore         JDBC 状态存储
     * @param handlerRegistry   处理器注册表
     * @param serializer        载荷序列化器
     * @param clock             系统时钟
     * @param leaseDuration     执行租约时长
     * @param heartbeatInterval 自动心跳间隔
     */
    public AsyncTaskProcessor(JdbcTaskStore taskStore,
                              AsyncTaskHandlerRegistry handlerRegistry,
                              TaskPayloadSerializer serializer,
                              Clock clock,
                              Duration leaseDuration,
                              Duration heartbeatInterval) {
        this(taskStore, handlerRegistry, serializer, clock, leaseDuration,
                heartbeatInterval, NoOpAsyncTaskObserver.INSTANCE);
    }

    /**
     * 创建带生命周期观测能力的异步任务处理器。
     *
     * @param taskStore         JDBC 状态存储
     * @param handlerRegistry   处理器注册表
     * @param serializer        载荷序列化器
     * @param clock             系统时钟
     * @param leaseDuration     执行租约时长
     * @param heartbeatInterval 自动心跳间隔
     * @param observer          任务生命周期观测器
     */
    public AsyncTaskProcessor(JdbcTaskStore taskStore,
                              AsyncTaskHandlerRegistry handlerRegistry,
                              TaskPayloadSerializer serializer,
                              Clock clock,
                              Duration leaseDuration,
                              Duration heartbeatInterval,
                              AsyncTaskObserver observer) {
        this(taskStore, handlerRegistry, serializer, clock, leaseDuration,
                heartbeatInterval, DEFAULT_HEARTBEAT_THREADS, observer);
    }

    /**
     * 创建带可配置心跳线程池和生命周期观测能力的异步任务处理器。
     *
     * @param taskStore         JDBC 状态存储
     * @param handlerRegistry   处理器注册表
     * @param serializer        载荷序列化器
     * @param clock             系统时钟
     * @param leaseDuration     执行租约时长
     * @param heartbeatInterval 自动心跳间隔
     * @param heartbeatThreads  自动心跳线程数
     * @param observer          任务生命周期观测器
     */
    public AsyncTaskProcessor(JdbcTaskStore taskStore,
                              AsyncTaskHandlerRegistry handlerRegistry,
                              TaskPayloadSerializer serializer,
                              Clock clock,
                              Duration leaseDuration,
                              Duration heartbeatInterval,
                              int heartbeatThreads,
                              AsyncTaskObserver observer) {
        this.taskStore = Objects.requireNonNull(taskStore, "JDBC 状态存储不能为空");
        this.handlerRegistry = Objects.requireNonNull(handlerRegistry, "处理器注册表不能为空");
        this.serializer = Objects.requireNonNull(serializer, "载荷序列化器不能为空");
        this.clock = Objects.requireNonNull(clock, "系统时钟不能为空");
        this.leaseDuration = Objects.requireNonNull(leaseDuration, "执行租约时长不能为空");
        this.heartbeatInterval = Objects.requireNonNull(heartbeatInterval, "自动心跳间隔不能为空");
        if (leaseDuration.isZero() || leaseDuration.isNegative()) {
            throw new IllegalArgumentException("执行租约时长必须大于 0");
        }
        if (heartbeatInterval.isZero() || heartbeatInterval.isNegative()) {
            throw new IllegalArgumentException("自动心跳间隔必须大于 0");
        }
        if (heartbeatInterval.compareTo(leaseDuration) >= 0) {
            throw new IllegalArgumentException("自动心跳间隔必须小于执行租约时长");
        }
        if (heartbeatThreads <= 0) {
            throw new IllegalArgumentException("自动心跳线程数必须大于 0");
        }
        this.observer = FailureIsolatingAsyncTaskObserver.wrap(observer);
        this.heartbeatExecutor = Executors.newScheduledThreadPool(
                heartbeatThreads,
                Thread.ofPlatform().daemon(true).name(HEARTBEAT_THREAD_NAME_PREFIX, 0).factory());
    }

    /**
     * 处理一条异步任务消息。
     *
     * @param envelope 消息信封
     * @return 本次实际执行处理器时返回 true，已完成或已死信时返回 false
     * @throws TaskExecutionInProgressException 任务正在其他实例执行时抛出
     * @throws AsyncTaskProcessingException     业务处理失败时抛出
     */
    public boolean process(AsyncTaskEnvelope envelope) {
        Optional<String> leaseToken = taskStore.claimExecution(
                envelope, clock.instant(), leaseDuration);
        if (leaseToken.isEmpty()) {
            return false;
        }
        executeClaimed(envelope, leaseToken.get());
        return true;
    }

    /**
     * 处理进入 Kafka 死信主题的任务。
     *
     * @param envelope 消息信封
     * @param reason   死信原因
     */
    public void processDeadLetter(AsyncTaskEnvelope envelope, String reason) {
        Objects.requireNonNull(envelope, "消息信封不能为空");
        boolean markedDead = taskStore.markOutboxDead(envelope, reason, clock.instant());
        if (!markedDead) {
            return;
        }
        observer.onDeadLetter(envelope.destination(), envelope.taskType());
    }

    /**
     * 停止自动心跳调度器。
     */
    @Override
    public void close() {
        heartbeatExecutor.shutdownNow();
    }

    private void executeClaimed(AsyncTaskEnvelope envelope, String leaseToken) {
        try {
            AsyncTaskHandler<?> handler = handlerRegistry.getRequired(
                    envelope.taskType(), envelope.schemaVersion());
            AsyncTaskContext context = context(envelope, leaseToken);
            try (ExecutionLeaseHeartbeat ignored = new ExecutionLeaseHeartbeat(
                    envelope.taskId(),
                    heartbeatExecutor,
                    heartbeatInterval,
                    () -> refreshLease(envelope, leaseToken))) {
                invokeHandler(handler, context, envelope.payloadJson());
            }
        } catch (TaskExecutionInProgressException exception) {
            throw exception;
        } catch (Exception exception) {
            boolean failed = taskStore.markExecutionFailed(
                    envelope.taskId(), leaseToken, exceptionMessage(exception), clock.instant());
            if (!failed) {
                TaskExecutionInProgressException leaseException = new TaskExecutionInProgressException(
                        "无法记录异步任务执行失败，执行租约已经失效: " + envelope.taskId());
                leaseException.addSuppressed(exception);
                throw leaseException;
            }
            observer.onExecutionFailed(envelope.destination(), envelope.taskType());
            throw new AsyncTaskProcessingException("异步任务执行失败: " + envelope.taskId(), exception);
        }
        boolean completed = taskStore.markExecutionCompleted(
                envelope.taskId(), leaseToken, clock.instant());
        if (!completed) {
            throw new TaskExecutionInProgressException(
                    "异步任务执行完成，但 outbox 执行租约已经失效: " + envelope.taskId());
        }
        observer.onExecutionSucceeded(envelope.destination(), envelope.taskType());
    }

    private AsyncTaskContext context(AsyncTaskEnvelope envelope, String leaseToken) {
        return new AsyncTaskContext(
                envelope.taskId(),
                envelope.taskType(),
                envelope.schemaVersion(),
                envelope.idempotencyKey(),
                envelope.headers(),
                () -> refreshLease(envelope, leaseToken),
                progress -> updateProgress(envelope, leaseToken, progress));
    }

    private void refreshLease(AsyncTaskEnvelope envelope, String leaseToken) {
        boolean refreshed = taskStore.heartbeatExecution(
                envelope.taskId(), leaseToken, clock.instant(), leaseDuration);
        if (!refreshed) {
            throw new TaskExecutionInProgressException("无法刷新异步任务执行租约: " + envelope.taskId());
        }
    }

    private void updateProgress(AsyncTaskEnvelope envelope, String leaseToken, Object progress) {
        String progressJson = serializer.serialize(progress);
        boolean updated = taskStore.updateExecutionProgress(
                envelope.taskId(), leaseToken, progressJson, clock.instant(), leaseDuration);
        if (!updated) {
            throw new TaskExecutionInProgressException("无法更新异步任务进度: " + envelope.taskId());
        }
    }

    private <T> void invokeHandler(AsyncTaskHandler<T> handler,
                                   AsyncTaskContext context,
                                   String payloadJson) throws Exception {
        T payload;
        try {
            payload = serializer.deserialize(payloadJson, handler.payloadType());
        } catch (RuntimeException exception) {
            throw new InvalidAsyncTaskMessageException("异步任务业务载荷无法反序列化", exception);
        }
        handler.handle(context, payload);
    }

    private static String exceptionMessage(Exception exception) {
        String message = exception.getMessage();
        return message == null || message.isBlank() ? exception.getClass().getName() : message;
    }

}
