package com.zaviwayne.asynctask.core;

/**
 * 任务心跳回调。
 *
 * @since 2026-08-26
 */
@FunctionalInterface
public interface TaskHeartbeat {
    /**
     * 刷新执行租约。
     */
    void refresh();
}
