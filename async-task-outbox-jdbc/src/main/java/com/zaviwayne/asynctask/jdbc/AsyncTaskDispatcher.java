package com.zaviwayne.asynctask.jdbc;

import com.zaviwayne.asynctask.core.AsyncTaskObserver;
import com.zaviwayne.asynctask.core.AsyncTaskTransport;
import com.zaviwayne.asynctask.core.AsyncTaskTransportException;
import com.zaviwayne.asynctask.core.NoOpAsyncTaskObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

/**
 * 异步任务 outbox 投递器。
 *
 * @since 2026-08-26
 */
public final class AsyncTaskDispatcher {
    /**
     * 日志记录器。
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(AsyncTaskDispatcher.class);

    /**
     * JDBC 状态存储。
     */
    private final JdbcTaskStore taskStore;

    /**
     * 消息传输通道。
     */
    private final AsyncTaskTransport transport;

    /**
     * 重试策略。
     */
    private final ExponentialBackoffPolicy retryPolicy;

    /**
     * 系统时钟。
     */
    private final Clock clock;

    /**
     * 每次抢占的记录数。
     */
    private final int batchSize;

    /**
     * 投递租约时长。
     */
    private final Duration leaseDuration;

    /**
     * 任务生命周期观测器。
     */
    private final AsyncTaskObserver observer;

    /**
     * 创建异步任务 outbox 投递器。
     *
     * @param taskStore     JDBC 状态存储
     * @param transport     消息传输通道
     * @param retryPolicy   重试策略
     * @param clock         系统时钟
     * @param batchSize     每次抢占的记录数
     * @param leaseDuration 投递租约时长
     */
    public AsyncTaskDispatcher(JdbcTaskStore taskStore,
                               AsyncTaskTransport transport,
                               ExponentialBackoffPolicy retryPolicy,
                               Clock clock,
                               int batchSize,
                               Duration leaseDuration) {
        this(taskStore, transport, retryPolicy, clock, batchSize, leaseDuration, NoOpAsyncTaskObserver.INSTANCE);
    }

    /**
     * 创建带生命周期观测能力的异步任务 outbox 投递器。
     *
     * @param taskStore     JDBC 状态存储
     * @param transport     消息传输通道
     * @param retryPolicy   重试策略
     * @param clock         系统时钟
     * @param batchSize     每次抢占的记录数
     * @param leaseDuration 投递租约时长
     * @param observer      任务生命周期观测器
     */
    public AsyncTaskDispatcher(JdbcTaskStore taskStore,
                               AsyncTaskTransport transport,
                               ExponentialBackoffPolicy retryPolicy,
                               Clock clock,
                               int batchSize,
                               Duration leaseDuration,
                               AsyncTaskObserver observer) {
        if (batchSize <= 0) {
            throw new IllegalArgumentException("批量大小必须大于 0");
        }
        this.taskStore = Objects.requireNonNull(taskStore, "JDBC 状态存储不能为空");
        this.transport = Objects.requireNonNull(transport, "消息传输通道不能为空");
        this.retryPolicy = Objects.requireNonNull(retryPolicy, "重试策略不能为空");
        this.clock = Objects.requireNonNull(clock, "系统时钟不能为空");
        this.batchSize = batchSize;
        this.leaseDuration = requirePositive(leaseDuration);
        this.observer = FailureIsolatingAsyncTaskObserver.wrap(observer);
    }

    /**
     * 抢占并投递一批 outbox 消息。
     */
    public void dispatchBatch() {
        Instant claimTime = clock.instant();
        String leaseToken = UUID.randomUUID().toString();
        taskStore.claimOutboxClaims(
                        batchSize, retryPolicy.maxAttempts(), claimTime, leaseDuration, leaseToken)
                .forEach(claim -> dispatch(claim, leaseToken));
    }

    private void dispatch(OutboxClaim claim, String leaseToken) {
        try {
            transport.send(claim.envelope());
        } catch (RuntimeException exception) {
            handleSendFailure(claim, leaseToken, exception);
            return;
        }
        try {
            boolean updated = taskStore.markOutboxDispatched(
                    claim.envelope().taskId(), leaseToken, clock.instant());
            if (!updated) {
                LOGGER.warn("异步任务投递成功，但 outbox 租约已经失效，任务可能被重复投递: taskId={}",
                        claim.envelope().taskId());
            } else {
                observer.onDispatchSucceeded(
                        claim.envelope().destination(), claim.envelope().taskType());
            }
        } catch (RuntimeException exception) {
            LOGGER.error("异步任务已经发送成功，但更新 outbox 投递状态失败，等待租约过期后接管: taskId={}",
                    claim.envelope().taskId(), exception);
        }
    }

    private void handleSendFailure(OutboxClaim claim, String leaseToken, RuntimeException exception) {
        Instant failedAt = clock.instant();
        boolean deliveryUncertain = claim.deliveryUncertain() || isDeliveryUncertain(exception);
        boolean terminal = retryPolicy.isExhausted(claim.attempt()) && !deliveryUncertain;
        Instant nextAttemptAt = terminal
                ? null
                : failedAt.plus(retryPolicy.nextDelay(claim.attempt()));
        boolean updated = deliveryUncertain
                ? taskStore.markOutboxDeliveryUncertain(
                claim.envelope().taskId(), leaseToken,
                exceptionMessage(exception), nextAttemptAt, failedAt)
                : taskStore.markOutboxFailed(
                claim.envelope().taskId(), leaseToken,
                exceptionMessage(exception), nextAttemptAt, failedAt);
        if (!updated) {
            LOGGER.warn("异步任务投递失败，但 outbox 租约已经失效，忽略旧实例的失败结果: taskId={}",
                    claim.envelope().taskId(), exception);
            return;
        }
        observer.onDispatchFailed(
                claim.envelope().destination(), claim.envelope().taskType(), terminal);
        LOGGER.warn("异步任务投递失败: taskId={}, attempt={}",
                claim.envelope().taskId(), claim.attempt(), exception);
    }

    private static String exceptionMessage(RuntimeException exception) {
        String message = exception.getMessage();
        return message == null || message.isBlank() ? exception.getClass().getName() : message;
    }

    private static boolean isDeliveryUncertain(RuntimeException exception) {
        return exception instanceof AsyncTaskTransportException transportException
                && transportException.isDeliveryUncertain();
    }

    private static Duration requirePositive(Duration duration) {
        Objects.requireNonNull(duration, "投递租约时长不能为空");
        if (duration.isZero() || duration.isNegative()) {
            throw new IllegalArgumentException("投递租约时长必须大于 0");
        }
        return duration;
    }
}
