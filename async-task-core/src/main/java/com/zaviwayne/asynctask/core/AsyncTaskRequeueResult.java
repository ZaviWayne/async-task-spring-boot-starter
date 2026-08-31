package com.zaviwayne.asynctask.core;

/**
 * 异步任务重新入队结果。
 *
 * @since 2026-08-27
 */
public enum AsyncTaskRequeueResult {
    /**
     * 已重新进入待投递状态。
     */
    REQUEUED,

    /**
     * 任务不存在。
     */
    NOT_FOUND,

    /**
     * 任务尚未进入允许重新入队的终态。
     */
    NOT_TERMINAL
}
