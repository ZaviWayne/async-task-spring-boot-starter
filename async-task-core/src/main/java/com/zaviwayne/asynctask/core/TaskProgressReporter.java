package com.zaviwayne.asynctask.core;

/**
 * 异步任务进度上报器。
 *
 * @since 2026-08-27
 */
@FunctionalInterface
public interface TaskProgressReporter {
    /**
     * 持久化当前任务进度。
     *
     * @param progress 可序列化的进度对象
     */
    void update(Object progress);
}
