package com.zaviwayne.asynctask.core;

import java.util.Map;
import java.util.Objects;

/**
 * 异步任务提交请求。
 *
 * @param destination    目标通道
 * @param taskType       任务类型
 * @param schemaVersion  消息结构版本
 * @param payload        业务载荷
 * @param idempotencyKey 幂等键
 * @param referenceType  业务关联类型，可为空
 * @param referenceId    业务关联标识，可为空
 * @param headers        扩展请求头
 * @param <T>            载荷类型
 * @since 2026-08-26
 */
public record AsyncTaskRequest<T>(String destination,
                                  String taskType,
                                  int schemaVersion,
                                  T payload,
                                  String idempotencyKey,
                                  String referenceType,
                                  String referenceId,
                                  Map<String, String> headers) {
    public AsyncTaskRequest {
        destination = AsyncTaskMessageValidator.validateDestination(destination);
        taskType = AsyncTaskMessageValidator.validateTaskType(taskType);
        if (schemaVersion <= 0) {
            throw new IllegalArgumentException("消息结构版本必须大于 0");
        }
        Objects.requireNonNull(payload, "业务载荷不能为空");
        idempotencyKey = AsyncTaskMessageValidator.validateIdempotencyKey(idempotencyKey);
        referenceType = AsyncTaskMessageValidator.normalizeReferenceType(referenceType);
        referenceId = AsyncTaskMessageValidator.normalizeReferenceId(referenceId);
        AsyncTaskMessageValidator.validateReferencePair(referenceType, referenceId);
        headers = AsyncTaskMessageValidator.validateHeaders(headers);
    }

    /**
     * 创建不带扩展请求头的异步任务请求。
     *
     * @param destination    目标通道
     * @param taskType       任务类型
     * @param schemaVersion  消息结构版本
     * @param payload        业务载荷
     * @param idempotencyKey 幂等键
     * @param <T>            载荷类型
     * @return 异步任务请求
     */
    public static <T> AsyncTaskRequest<T> of(String destination, String taskType, int schemaVersion,
                                             T payload, String idempotencyKey) {
        return new AsyncTaskRequest<>(
                destination, taskType, schemaVersion, payload, idempotencyKey, null, null, Map.of());
    }

    /**
     * 创建带业务关联信息且不带扩展请求头的异步任务请求。
     *
     * @param destination    目标通道
     * @param taskType       任务类型
     * @param schemaVersion  消息结构版本
     * @param payload        业务载荷
     * @param idempotencyKey 幂等键
     * @param referenceType  业务关联类型
     * @param referenceId    业务关联标识
     * @param <T>            载荷类型
     * @return 异步任务请求
     */
    public static <T> AsyncTaskRequest<T> referenced(String destination,
                                                     String taskType,
                                                     int schemaVersion,
                                                     T payload,
                                                     String idempotencyKey,
                                                     String referenceType,
                                                     String referenceId) {
        return new AsyncTaskRequest<>(destination, taskType, schemaVersion, payload, idempotencyKey,
                referenceType, referenceId, Map.of());
    }

}
