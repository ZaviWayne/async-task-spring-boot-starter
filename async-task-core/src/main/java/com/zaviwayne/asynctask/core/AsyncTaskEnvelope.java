package com.zaviwayne.asynctask.core;

import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

/**
 * 版本化异步任务消息信封。
 *
 * @param taskId         任务 ID
 * @param destination    目标通道
 * @param taskType       任务类型
 * @param schemaVersion  消息结构版本
 * @param payloadJson    JSON 载荷
 * @param payloadHash    载荷摘要
 * @param idempotencyKey 幂等键
 * @param referenceType  业务关联类型，可为空
 * @param referenceId    业务关联标识，可为空
 * @param headers        扩展请求头
 * @param generation     重新入队代际，从 0 开始
 * @param createdAt      创建时间
 * @since 2026-08-26
 */
public record AsyncTaskEnvelope(UUID taskId,
                                String destination,
                                String taskType,
                                int schemaVersion,
                                String payloadJson,
                                String payloadHash,
                                String idempotencyKey,
                                String referenceType,
                                String referenceId,
                                Map<String, String> headers,
                                int generation,
                                Instant createdAt) {
    public AsyncTaskEnvelope {
        Objects.requireNonNull(taskId, "任务 ID 不能为空");
        destination = AsyncTaskMessageValidator.validateDestination(destination);
        taskType = AsyncTaskMessageValidator.validateTaskType(taskType);
        if (schemaVersion <= 0) {
            throw new IllegalArgumentException("消息结构版本必须大于 0");
        }
        payloadJson = AsyncTaskMessageValidator.validatePayloadJson(payloadJson);
        payloadHash = AsyncTaskMessageValidator.validatePayloadHash(payloadJson, payloadHash);
        idempotencyKey = AsyncTaskMessageValidator.validateIdempotencyKey(idempotencyKey);
        referenceType = AsyncTaskMessageValidator.normalizeReferenceType(referenceType);
        referenceId = AsyncTaskMessageValidator.normalizeReferenceId(referenceId);
        AsyncTaskMessageValidator.validateReferencePair(referenceType, referenceId);
        headers = AsyncTaskMessageValidator.validateHeaders(headers);
        if (generation < 0) {
            throw new IllegalArgumentException("重新入队代际不能小于 0");
        }
        Objects.requireNonNull(createdAt, "创建时间不能为空");
    }

    /**
     * 创建初始代际的异步任务信封。
     *
     * @param taskId         任务 ID
     * @param destination    目标通道
     * @param taskType       任务类型
     * @param schemaVersion  消息结构版本
     * @param payloadJson    JSON 载荷
     * @param payloadHash    载荷摘要
     * @param idempotencyKey 幂等键
     * @param referenceType  业务关联类型，可为空
     * @param referenceId    业务关联标识，可为空
     * @param headers        扩展请求头
     * @param createdAt      创建时间
     */
    public AsyncTaskEnvelope(UUID taskId,
                             String destination,
                             String taskType,
                             int schemaVersion,
                             String payloadJson,
                             String payloadHash,
                             String idempotencyKey,
                             String referenceType,
                             String referenceId,
                             Map<String, String> headers,
                             Instant createdAt) {
        this(taskId, destination, taskType, schemaVersion, payloadJson, payloadHash,
                idempotencyKey, referenceType, referenceId, headers, 0, createdAt);
    }

    /**
     * 创建下一重新入队代际的消息信封。
     *
     * @return 代际递增后的消息信封
     * @throws IllegalStateException 当前代际已经达到整数上限时抛出
     */
    public AsyncTaskEnvelope nextGeneration() {
        if (generation == Integer.MAX_VALUE) {
            throw new IllegalStateException("异步任务重新入队代际已达到上限");
        }
        return new AsyncTaskEnvelope(
                taskId, destination, taskType, schemaVersion, payloadJson, payloadHash,
                idempotencyKey, referenceType, referenceId, headers, generation + 1, createdAt);
    }
}
