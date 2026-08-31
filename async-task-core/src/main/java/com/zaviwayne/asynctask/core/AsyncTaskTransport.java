package com.zaviwayne.asynctask.core;

/**
 * 异步任务传输通道。
 *
 * @since 2026-08-26
 */
public interface AsyncTaskTransport {
    /**
     * 将任务信封发送到目标通道。
     *
     * @param envelope 任务信封
     * @throws AsyncTaskTransportException 发送失败时抛出
     */
    void send(AsyncTaskEnvelope envelope);
}
