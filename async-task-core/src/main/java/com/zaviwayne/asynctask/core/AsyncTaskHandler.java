package com.zaviwayne.asynctask.core;

/**
 * 异步任务处理器。
 *
 * @param <T> 载荷类型
 * @since 2026-08-26
 */
public interface AsyncTaskHandler<T> {
    /**
     * 获取处理的任务类型。
     *
     * @return 任务类型
     */
    String taskType();

    /**
     * 获取支持的消息结构版本。
     *
     * @return 消息结构版本
     */
    int schemaVersion();

    /**
     * 获取载荷 Java 类型。
     *
     * @return 载荷类型
     */
    Class<T> payloadType();

    /**
     * 处理异步任务。
     *
     * @param context 任务上下文
     * @param payload 业务载荷
     * @throws Exception 业务处理失败时抛出
     */
    void handle(AsyncTaskContext context, T payload) throws Exception;
}
