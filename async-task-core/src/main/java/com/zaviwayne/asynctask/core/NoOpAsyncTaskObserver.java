package com.zaviwayne.asynctask.core;

/**
 * 无操作异步任务生命周期观察器。
 *
 * @since 2026-08-31
 */
public final class NoOpAsyncTaskObserver implements AsyncTaskObserver {
    /**
     * 共享实例。
     */
    public static final NoOpAsyncTaskObserver INSTANCE = new NoOpAsyncTaskObserver();

    private NoOpAsyncTaskObserver() {
    }

    @Override
    public void onEnqueued(String destination, String taskType) {
    }

    @Override
    public void onDispatchSucceeded(String destination, String taskType) {
    }

    @Override
    public void onDispatchFailed(String destination, String taskType, boolean terminal) {
    }

    @Override
    public void onExecutionSucceeded(String destination, String taskType) {
    }

    @Override
    public void onExecutionFailed(String destination, String taskType) {
    }

    @Override
    public void onDeadLetter(String destination, String taskType) {
    }

    @Override
    public void onDispatchLeaseRecovered(String destination, String taskType) {
    }

    @Override
    public void onExecutionLeaseRecovered(String destination, String taskType) {
    }

    @Override
    public void onRequeued(String destination, String taskType) {
    }

    @Override
    public void onCleaned(int count) {
    }
}
