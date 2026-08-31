package com.zaviwayne.asynctask.core;

/**
 * 异步任务生命周期观测扩展点。
 *
 * @since 2026-08-27
 */
public interface AsyncTaskObserver {
    /**
     * 记录任务成功入队。
     *
     * @param destination 目标通道
     * @param taskType    任务类型
     */
    void onEnqueued(String destination, String taskType);

    /**
     * 记录任务投递成功。
     *
     * @param destination 目标通道
     * @param taskType    任务类型
     */
    void onDispatchSucceeded(String destination, String taskType);

    /**
     * 记录任务投递失败。
     *
     * @param destination 目标通道
     * @param taskType    任务类型
     * @param terminal    是否已经进入死信终态
     */
    void onDispatchFailed(String destination, String taskType, boolean terminal);

    /**
     * 记录任务执行成功。
     *
     * @param destination 目标通道
     * @param taskType    任务类型
     */
    void onExecutionSucceeded(String destination, String taskType);

    /**
     * 记录任务执行失败。
     *
     * @param destination 目标通道
     * @param taskType    任务类型
     */
    void onExecutionFailed(String destination, String taskType);

    /**
     * 记录任务进入死信终态。
     *
     * @param destination 目标通道
     * @param taskType    任务类型
     */
    void onDeadLetter(String destination, String taskType);

    /**
     * 记录投递租约接管。
     *
     * @param destination 目标通道
     * @param taskType    任务类型
     */
    void onDispatchLeaseRecovered(String destination, String taskType);

    /**
     * 记录执行租约接管。
     *
     * @param destination 目标通道
     * @param taskType    任务类型
     */
    void onExecutionLeaseRecovered(String destination, String taskType);

    /**
     * 记录终态任务重新入队。
     *
     * @param destination 目标通道
     * @param taskType    任务类型
     */
    void onRequeued(String destination, String taskType);

    /**
     * 记录终态任务清理数量。
     *
     * @param count 清理数量
     */
    void onCleaned(int count);
}
