package com.zaviwayne.asynctask.jdbc;

import com.zaviwayne.asynctask.core.AsyncTaskObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

/**
 * 隔离观测回调异常的异步任务观测器。
 *
 * @since 2026-08-28
 */
final class FailureIsolatingAsyncTaskObserver implements AsyncTaskObserver {
    /**
     * 日志记录器。
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(FailureIsolatingAsyncTaskObserver.class);

    /**
     * 实际观测器。
     */
    private final AsyncTaskObserver delegate;

    private FailureIsolatingAsyncTaskObserver(AsyncTaskObserver delegate) {
        this.delegate = delegate;
    }

    /**
     * 包装观测器并隔离其运行时异常。
     *
     * @param observer 原始观测器
     * @return 已具备异常隔离能力的观测器
     */
    public static AsyncTaskObserver wrap(AsyncTaskObserver observer) {
        AsyncTaskObserver requiredObserver = Objects.requireNonNull(observer, "任务生命周期观测器不能为空");
        if (requiredObserver instanceof FailureIsolatingAsyncTaskObserver) {
            return requiredObserver;
        }
        return new FailureIsolatingAsyncTaskObserver(requiredObserver);
    }

    @Override
    public void onEnqueued(String destination, String taskType) {
        observe(() -> delegate.onEnqueued(destination, taskType));
    }

    @Override
    public void onDispatchSucceeded(String destination, String taskType) {
        observe(() -> delegate.onDispatchSucceeded(destination, taskType));
    }

    @Override
    public void onDispatchFailed(String destination, String taskType, boolean terminal) {
        observe(() -> delegate.onDispatchFailed(destination, taskType, terminal));
    }

    @Override
    public void onExecutionSucceeded(String destination, String taskType) {
        observe(() -> delegate.onExecutionSucceeded(destination, taskType));
    }

    @Override
    public void onExecutionFailed(String destination, String taskType) {
        observe(() -> delegate.onExecutionFailed(destination, taskType));
    }

    @Override
    public void onDeadLetter(String destination, String taskType) {
        observe(() -> delegate.onDeadLetter(destination, taskType));
    }

    @Override
    public void onDispatchLeaseRecovered(String destination, String taskType) {
        observe(() -> delegate.onDispatchLeaseRecovered(destination, taskType));
    }

    @Override
    public void onExecutionLeaseRecovered(String destination, String taskType) {
        observe(() -> delegate.onExecutionLeaseRecovered(destination, taskType));
    }

    @Override
    public void onRequeued(String destination, String taskType) {
        observe(() -> delegate.onRequeued(destination, taskType));
    }

    @Override
    public void onCleaned(int count) {
        observe(() -> delegate.onCleaned(count));
    }

    private static void observe(Runnable callback) {
        try {
            callback.run();
        } catch (RuntimeException exception) {
            LOGGER.warn("异步任务观测回调执行失败，已忽略", exception);
        }
    }
}
