package com.zaviwayne.asynctask.jdbc;

import com.zaviwayne.asynctask.core.*;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import java.time.Clock;
import java.time.Instant;
import java.util.Objects;
import java.util.UUID;
import java.util.function.Supplier;

/**
 * JDBC 事务发件箱入队器。
 *
 * @since 2026-08-26
 */
public final class JdbcAsyncTaskEnqueuer implements AsyncTaskEnqueuer {
    /**
     * JDBC 状态存储。
     */
    private final JdbcTaskStore taskStore;

    /**
     * 载荷序列化器。
     */
    private final TaskPayloadSerializer serializer;

    /**
     * 系统时钟。
     */
    private final Clock clock;

    /**
     * 任务 ID 生成器。
     */
    private final Supplier<UUID> taskIdSupplier;

    /**
     * 创建 JDBC 事务发件箱入队器。
     *
     * @param taskStore  JDBC 状态存储
     * @param serializer 载荷序列化器
     * @param clock      系统时钟
     */
    public JdbcAsyncTaskEnqueuer(JdbcTaskStore taskStore, TaskPayloadSerializer serializer, Clock clock) {
        this(taskStore, serializer, clock, UUID::randomUUID);
    }

    private JdbcAsyncTaskEnqueuer(JdbcTaskStore taskStore,
                                  TaskPayloadSerializer serializer,
                                  Clock clock,
                                  Supplier<UUID> taskIdSupplier) {
        this.taskStore = Objects.requireNonNull(taskStore, "JDBC 状态存储不能为空");
        this.serializer = Objects.requireNonNull(serializer, "载荷序列化器不能为空");
        this.clock = Objects.requireNonNull(clock, "系统时钟不能为空");
        this.taskIdSupplier = Objects.requireNonNull(taskIdSupplier, "任务 ID 生成器不能为空");
    }

    /**
     * 在当前业务事务中持久化异步任务。
     *
     * @param request 异步任务请求
     * @param <T>     载荷类型
     * @return 新任务或同一幂等任务的 ID
     * @throws IllegalStateException 当前不存在活动事务时抛出
     */
    @Override
    public <T> UUID enqueue(AsyncTaskRequest<T> request) {
        Objects.requireNonNull(request, "异步任务请求不能为空");
        boolean transactionActive = TransactionSynchronizationManager.isActualTransactionActive()
                && taskStore.isDatabaseTransactionActive();
        if (!transactionActive) {
            throw new IllegalStateException("异步任务入队必须在活动的数据库事务中执行");
        }
        String payloadJson = serializer.serialize(request.payload());
        Instant createdAt = clock.instant();
        AsyncTaskEnvelope envelope = new AsyncTaskEnvelope(
                taskIdSupplier.get(),
                request.destination(),
                request.taskType(),
                request.schemaVersion(),
                payloadJson,
                AsyncTaskMessageValidator.calculatePayloadHash(payloadJson),
                request.idempotencyKey(),
                request.referenceType(),
                request.referenceId(),
                request.headers(),
                createdAt);
        return taskStore.saveOutbox(envelope);
    }
}
