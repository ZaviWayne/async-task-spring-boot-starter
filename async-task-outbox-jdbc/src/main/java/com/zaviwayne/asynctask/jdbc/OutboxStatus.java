package com.zaviwayne.asynctask.jdbc;

import com.zaviwayne.asynctask.core.AsyncTaskStatus;

/**
 * Outbox 数据库存储状态。
 *
 * @since 2026-08-31
 */
enum OutboxStatus {
    /**
     * 等待投递。
     */
    PENDING(0, AsyncTaskStatus.PENDING),

    /**
     * 已获取投递租约。
     */
    DISPATCHING(1, AsyncTaskStatus.DISPATCHING),

    /**
     * 已完成投递。
     */
    DISPATCHED(2, AsyncTaskStatus.DISPATCHED),

    /**
     * 等待重试。
     */
    RETRY(3, AsyncTaskStatus.RETRY),

    /**
     * 已进入死信终态。
     */
    DEAD(4, AsyncTaskStatus.DEAD),

    /**
     * 正在执行。
     */
    RUNNING(5, AsyncTaskStatus.RUNNING),

    /**
     * 已执行成功。
     */
    SUCCESS(6, AsyncTaskStatus.SUCCESS),

    /**
     * 执行失败，等待消息重投。
     */
    FAILED(7, AsyncTaskStatus.FAILED),

    /**
     * 投递结果未知，等待再次投递。
     */
    DELIVERY_UNCERTAIN(8, AsyncTaskStatus.RETRY);

    /**
     * 数据库存储状态码。
     */
    private final int code;

    /**
     * 对外任务状态。
     */
    private final AsyncTaskStatus publicStatus;

    OutboxStatus(int code, AsyncTaskStatus publicStatus) {
        this.code = code;
        this.publicStatus = publicStatus;
    }

    /**
     * 获取数据库存储状态码。
     *
     * @return 数据库存储状态码
     */
    public int getCode() {
        return code;
    }

    /**
     * 转换为对外任务状态。
     *
     * @return 对外任务状态
     */
    public AsyncTaskStatus toPublicStatus() {
        return publicStatus;
    }

    /**
     * 根据数据库状态码解析内部状态。
     *
     * @param code 数据库存储状态码
     * @return 数据库内部状态
     * @throws IllegalArgumentException 状态码未知时抛出
     */
    public static OutboxStatus fromCode(int code) {
        for (OutboxStatus status : values()) {
            if (status.code == code) {
                return status;
            }
        }
        throw new IllegalArgumentException("未知的异步任务状态码: " + code);
    }
}
