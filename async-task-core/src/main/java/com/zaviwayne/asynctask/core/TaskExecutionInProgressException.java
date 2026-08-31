package com.zaviwayne.asynctask.core;

/**
 * 异步任务正在执行异常。
 *
 * @since 2026-08-26
 */
public class TaskExecutionInProgressException extends AsyncTaskException {
    /**
     * 创建异步任务正在执行异常。
     *
     * @param message 异常消息
     */
    public TaskExecutionInProgressException(String message) {
        super(message);
    }
}
