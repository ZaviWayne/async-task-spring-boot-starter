package com.zaviwayne.asynctask.core;

import java.util.*;

/**
 * 异步任务执行上下文。
 *
 * @since 2026-08-26
 */
public final class AsyncTaskContext {
    /**
     * 任务 ID。
     */
    private final UUID taskId;

    /**
     * 任务类型。
     */
    private final String taskType;

    /**
     * 消息结构版本。
     */
    private final int schemaVersion;

    /**
     * 幂等键。
     */
    private final String idempotencyKey;

    /**
     * 不可变扩展请求头。
     */
    private final Map<String, String> headers;

    /**
     * 内部心跳回调。
     */
    private final TaskHeartbeat heartbeatCallback;

    /**
     * 内部进度上报器。
     */
    private final TaskProgressReporter progressReporter;

    /**
     * 创建异步任务执行上下文。
     *
     * @param taskId            任务 ID
     * @param taskType          任务类型
     * @param schemaVersion     消息结构版本
     * @param idempotencyKey    幂等键
     * @param headers           扩展请求头
     * @param heartbeatCallback 心跳回调
     * @param progressReporter  进度上报器
     */
    public AsyncTaskContext(UUID taskId,
                            String taskType,
                            int schemaVersion,
                            String idempotencyKey,
                            Map<String, String> headers,
                            TaskHeartbeat heartbeatCallback,
                            TaskProgressReporter progressReporter) {
        this.taskId = Objects.requireNonNull(taskId, "任务 ID 不能为空");
        this.taskType = Objects.requireNonNull(taskType, "任务类型不能为空");
        if (schemaVersion <= 0) {
            throw new IllegalArgumentException("消息结构版本必须大于 0");
        }
        this.schemaVersion = schemaVersion;
        this.idempotencyKey = Objects.requireNonNull(idempotencyKey, "幂等键不能为空");
        this.headers = headers == null || headers.isEmpty()
                ? Map.of()
                : Collections.unmodifiableMap(new LinkedHashMap<>(headers));
        this.heartbeatCallback = Objects.requireNonNull(heartbeatCallback, "心跳回调不能为空");
        this.progressReporter = Objects.requireNonNull(progressReporter, "进度上报器不能为空");
    }

    /**
     * 获取任务 ID。
     *
     * @return 任务 ID
     */
    public UUID taskId() {
        return taskId;
    }

    /**
     * 获取任务类型。
     *
     * @return 任务类型
     */
    public String taskType() {
        return taskType;
    }

    /**
     * 获取消息结构版本。
     *
     * @return 消息结构版本
     */
    public int schemaVersion() {
        return schemaVersion;
    }

    /**
     * 获取幂等键。
     *
     * @return 幂等键
     */
    public String idempotencyKey() {
        return idempotencyKey;
    }

    /**
     * 获取不可变扩展请求头。
     *
     * @return 扩展请求头
     */
    public Map<String, String> headers() {
        return headers;
    }

    /**
     * 刷新当前任务的执行租约。
     */
    public void heartbeat() {
        heartbeatCallback.refresh();
    }

    /**
     * 持久化当前任务进度并刷新执行租约。
     *
     * @param progress 可序列化的进度对象
     */
    public void updateProgress(Object progress) {
        progressReporter.update(Objects.requireNonNull(progress, "任务进度不能为空"));
    }
}
