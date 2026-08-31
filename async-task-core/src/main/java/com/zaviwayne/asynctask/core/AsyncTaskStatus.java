package com.zaviwayne.asynctask.core;

/**
 * 异步任务生命周期状态。
 *
 * @since 2026-08-27
 */
public enum AsyncTaskStatus {
    /**
     * 等待投递。
     */
    PENDING,

    /**
     * 正在投递。
     */
    DISPATCHING,

    /**
     * 已完成投递。
     */
    DISPATCHED,

    /**
     * 等待投递重试。
     */
    RETRY,

    /**
     * 已进入死信终态。
     */
    DEAD,

    /**
     * 正在执行。
     */
    RUNNING,

    /**
     * 已执行成功。
     */
    SUCCESS,

    /**
     * 执行失败，等待消息重投。
     */
    FAILED
}
