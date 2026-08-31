package com.zaviwayne.asynctask.kafka;

import com.zaviwayne.asynctask.core.*;
import org.springframework.kafka.core.KafkaOperations;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Kafka 异步任务传输通道。
 *
 * @since 2026-08-26
 */
public final class KafkaAsyncTaskTransport implements AsyncTaskTransport {
    /**
     * Kafka 操作接口。
     */
    private final KafkaOperations<Object, Object> kafkaOperations;

    /**
     * 消息序列化器。
     */
    private final TaskPayloadSerializer serializer;

    /**
     * 任务内容大小限制。
     */
    private final AsyncTaskContentLimits contentLimits;

    /**
     * 发送确认超时时间。
     */
    private final Duration sendTimeout;

    /**
     * 是否使用 Kafka 事务发送。
     */
    private final boolean transactionEnabled;

    /**
     * 创建 Kafka 异步任务传输通道。
     *
     * @param kafkaOperations    Kafka 操作接口
     * @param serializer         消息序列化器
     * @param sendTimeout        发送确认超时时间
     * @param transactionEnabled 是否使用 Kafka 事务发送
     */
    public KafkaAsyncTaskTransport(KafkaOperations<Object, Object> kafkaOperations,
                                   TaskPayloadSerializer serializer,
                                   Duration sendTimeout,
                                   boolean transactionEnabled) {
        this(kafkaOperations, serializer, sendTimeout,
                transactionEnabled, AsyncTaskContentLimits.defaults());
    }

    /**
     * 创建带内容限制的 Kafka 异步任务传输通道。
     *
     * @param kafkaOperations    Kafka 操作接口
     * @param serializer         消息序列化器
     * @param sendTimeout        发送确认超时时间
     * @param transactionEnabled 是否使用 Kafka 事务发送
     * @param contentLimits      任务内容大小限制
     */
    public KafkaAsyncTaskTransport(KafkaOperations<Object, Object> kafkaOperations,
                                   TaskPayloadSerializer serializer,
                                   Duration sendTimeout,
                                   boolean transactionEnabled,
                                   AsyncTaskContentLimits contentLimits) {
        this.kafkaOperations = Objects.requireNonNull(kafkaOperations, "Kafka 操作接口不能为空");
        this.serializer = Objects.requireNonNull(serializer, "消息序列化器不能为空");
        this.contentLimits = Objects.requireNonNull(contentLimits, "任务内容大小限制不能为空");
        this.sendTimeout = requireAtLeastOneMillisecond(sendTimeout);
        this.transactionEnabled = transactionEnabled;
        if (transactionEnabled && !kafkaOperations.isTransactional()) {
            throw new IllegalArgumentException("启用 Kafka 事务发送时必须配置事务型 KafkaTemplate");
        }
    }

    /**
     * 创建使用普通 Kafka 发送的任务传输通道。
     *
     * @param kafkaOperations Kafka 操作接口
     * @param serializer      消息序列化器
     * @param sendTimeout     发送确认超时时间
     */
    public KafkaAsyncTaskTransport(KafkaOperations<Object, Object> kafkaOperations,
                                   TaskPayloadSerializer serializer,
                                   Duration sendTimeout) {
        this(kafkaOperations, serializer, sendTimeout, false);
    }

    /**
     * 将任务信封作为 JSON 字符串发送到 Kafka。
     *
     * @param envelope 任务信封
     * @throws AsyncTaskTransportException 发送失败或等待确认超时时抛出
     */
    @Override
    public void send(AsyncTaskEnvelope envelope) {
        Objects.requireNonNull(envelope, "任务信封不能为空");
        String envelopeJson = serializer.serialize(envelope);
        contentLimits.validateEnvelopeJson(envelopeJson);
        if (transactionEnabled) {
            sendInTransaction(envelope, envelopeJson);
            return;
        }
        sendAndAwait(kafkaOperations, envelope, envelopeJson);
    }

    private void sendInTransaction(AsyncTaskEnvelope envelope, String envelopeJson) {
        try {
            kafkaOperations.executeInTransaction(operations -> {
                sendAndAwait(operations, envelope, envelopeJson);
                return null;
            });
        } catch (AsyncTaskTransportException exception) {
            throw exception;
        } catch (RuntimeException exception) {
            throw new AsyncTaskTransportException(
                    "Kafka 异步任务事务发送失败", exception, hasTimeoutCause(exception));
        }
    }

    private void sendAndAwait(KafkaOperations<Object, Object> operations,
                              AsyncTaskEnvelope envelope,
                              String envelopeJson) {
        try {
            operations.send(envelope.destination(), envelope.idempotencyKey(), envelopeJson)
                    .get(sendTimeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new AsyncTaskTransportException(
                    "等待 Kafka 发送确认时线程被中断", exception, true);
        } catch (TimeoutException exception) {
            throw new AsyncTaskTransportException("等待 Kafka 发送确认超时", exception, true);
        } catch (ExecutionException exception) {
            throw new AsyncTaskTransportException(
                    "Kafka 异步任务发送失败", exception, hasTimeoutCause(exception));
        }
    }

    private static boolean hasTimeoutCause(Throwable throwable) {
        Throwable current = throwable;
        while (current != null) {
            if (current instanceof TimeoutException
                    || current instanceof org.apache.kafka.common.errors.TimeoutException) {
                return true;
            }
            if (current.getCause() == current) {
                return false;
            }
            current = current.getCause();
        }
        return false;
    }

    private static Duration requireAtLeastOneMillisecond(Duration duration) {
        Objects.requireNonNull(duration, "发送确认超时时间不能为空");
        if (duration.isZero() || duration.isNegative()) {
            throw new IllegalArgumentException("发送确认超时时间必须大于 0");
        }
        if (duration.toMillis() == 0L) {
            throw new IllegalArgumentException("发送确认超时时间必须至少为 1 毫秒");
        }
        return duration;
    }
}
