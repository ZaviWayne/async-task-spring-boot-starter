package com.zaviwayne.asynctask.core;

/**
 * 异步任务基础异常。
 *
 * @since 2026-08-26
 */
public class AsyncTaskException extends RuntimeException {
    /**
     * 创建异步任务异常。
     *
     * @param message 异常消息
     */
    public AsyncTaskException(String message) {
        super(message);
    }

    /**
     * 创建带原因的异步任务异常。
     *
     * @param message 异常消息
     * @param cause   原始异常
     */
    public AsyncTaskException(String message, Throwable cause) {
        super(message, cause);
    }
}
