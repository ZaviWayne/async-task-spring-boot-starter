package com.zaviwayne.asynctask.kafka;

import com.zaviwayne.asynctask.core.AsyncTaskContentLimits;
import com.zaviwayne.asynctask.core.AsyncTaskEnvelope;
import com.zaviwayne.asynctask.core.InvalidAsyncTaskMessageException;
import com.zaviwayne.asynctask.core.TaskPayloadSerializer;
import com.zaviwayne.asynctask.jdbc.AsyncTaskProcessor;

import java.util.Objects;

/**
 * Kafka 异步任务消息入口。
 *
 * @since 2026-08-26
 */
public final class AsyncTaskKafkaMessageHandler {
    /**
     * 消息序列化器。
     */
    private final TaskPayloadSerializer serializer;

    /**
     * 任务内容大小限制。
     */
    private final AsyncTaskContentLimits contentLimits;

    /**
     * 异步任务处理器。
     */
    private final AsyncTaskProcessor taskProcessor;

    /**
     * 创建 Kafka 异步任务消息入口。
     *
     * @param serializer    消息序列化器
     * @param taskProcessor 异步任务处理器
     */
    public AsyncTaskKafkaMessageHandler(TaskPayloadSerializer serializer,
                                        AsyncTaskProcessor taskProcessor) {
        this(serializer, taskProcessor, AsyncTaskContentLimits.defaults());
    }

    /**
     * 创建带内容限制的 Kafka 异步任务消息入口。
     *
     * @param serializer    消息序列化器
     * @param taskProcessor 异步任务处理器
     * @param contentLimits 任务内容大小限制
     */
    public AsyncTaskKafkaMessageHandler(TaskPayloadSerializer serializer,
                                        AsyncTaskProcessor taskProcessor,
                                        AsyncTaskContentLimits contentLimits) {
        this.serializer = Objects.requireNonNull(serializer, "消息序列化器不能为空");
        this.taskProcessor = Objects.requireNonNull(taskProcessor, "异步任务处理器不能为空");
        this.contentLimits = Objects.requireNonNull(contentLimits, "任务内容大小限制不能为空");
    }

    /**
     * 处理普通 Kafka 消息。
     *
     * @param envelopeJson 任务信封 JSON
     * @throws InvalidAsyncTaskMessageException 任务信封无效时抛出
     */
    public void handle(String envelopeJson) {
        AsyncTaskEnvelope envelope = deserializeEnvelope(envelopeJson);
        taskProcessor.process(envelope);
    }

    /**
     * 校验 Kafka 路由身份并处理普通消息。
     *
     * @param actualTopic  实际消费主题
     * @param key          Kafka 消息 Key
     * @param envelopeJson 任务信封 JSON
     * @throws InvalidAsyncTaskMessageException 任务信封或路由身份无效时抛出
     */
    public void handle(String actualTopic, Object key, String envelopeJson) {
        AsyncTaskEnvelope envelope = deserializeEnvelope(envelopeJson);
        validateRouteIdentity(actualTopic, key, envelope);
        taskProcessor.process(envelope);
    }

    /**
     * 处理 Kafka 死信消息。
     *
     * @param envelopeJson 任务信封 JSON
     * @param reason       死信原因
     * @throws InvalidAsyncTaskMessageException 任务信封无效时抛出
     */
    public void handleDeadLetter(String envelopeJson, String reason) {
        AsyncTaskEnvelope envelope = deserializeEnvelope(envelopeJson);
        taskProcessor.processDeadLetter(envelope, reason);
    }

    /**
     * 校验 Kafka 路由身份并处理死信消息。
     *
     * @param expectedDestination 死信绑定对应的业务主题
     * @param key                 Kafka 消息 Key
     * @param envelopeJson        任务信封 JSON
     * @param reason              死信原因
     * @throws InvalidAsyncTaskMessageException 任务信封或路由身份无效时抛出
     */
    public void handleDeadLetter(String expectedDestination,
                                 Object key,
                                 String envelopeJson,
                                 String reason) {
        AsyncTaskEnvelope envelope = deserializeEnvelope(envelopeJson);
        validateRouteIdentity(expectedDestination, key, envelope);
        taskProcessor.processDeadLetter(envelope, reason);
    }

    private AsyncTaskEnvelope deserializeEnvelope(String envelopeJson) {
        try {
            contentLimits.validateEnvelopeJson(envelopeJson);
            return serializer.deserialize(envelopeJson, AsyncTaskEnvelope.class);
        } catch (RuntimeException exception) {
            throw new InvalidAsyncTaskMessageException(exceptionMessage(exception), exception);
        }
    }

    private static void validateRouteIdentity(String expectedDestination,
                                              Object key,
                                              AsyncTaskEnvelope envelope) {
        Objects.requireNonNull(expectedDestination, "Kafka 业务主题不能为空");
        if (!expectedDestination.equals(envelope.destination())) {
            throw new InvalidAsyncTaskMessageException(
                    "Kafka 业务主题与任务信封目标通道不一致: topic=" + expectedDestination
                            + ", destination=" + envelope.destination());
        }
        if (!(key instanceof String stringKey)) {
            throw new InvalidAsyncTaskMessageException("Kafka 异步任务消息 Key 必须是字符串");
        }
        if (!stringKey.equals(envelope.idempotencyKey())) {
            throw new InvalidAsyncTaskMessageException("Kafka 消息 Key 与任务信封幂等键不一致");
        }
    }

    private static String exceptionMessage(RuntimeException exception) {
        String message = exception.getMessage();
        return message == null || message.isBlank() ? "Kafka 异步任务信封无效" : message;
    }
}
