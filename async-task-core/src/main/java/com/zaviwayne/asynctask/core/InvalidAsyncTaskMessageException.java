package com.zaviwayne.asynctask.core;

/**
 * 不可重试的异步任务消息异常。
 *
 * @since 2026-08-31
 */
public class InvalidAsyncTaskMessageException extends AsyncTaskException {
    /**
     * 创建不可重试的异步任务消息异常。
     *
     * @param message 异常消息
     */
    public InvalidAsyncTaskMessageException(String message) {
        super(message);
    }

    /**
     * 创建带原始异常的不可重试异步任务消息异常。
     *
     * @param message 异常消息
     * @param cause   原始异常
     */
    public InvalidAsyncTaskMessageException(String message, Throwable cause) {
        super(message, cause);
    }
}
