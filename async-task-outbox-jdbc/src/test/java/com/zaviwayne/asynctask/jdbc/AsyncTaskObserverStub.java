package com.zaviwayne.asynctask.jdbc;

import com.zaviwayne.asynctask.core.AsyncTaskObserver;

/**
 * 测试用异步任务观察器空实现。
 *
 * @since 2026-08-31
 */
class AsyncTaskObserverStub implements AsyncTaskObserver {
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
