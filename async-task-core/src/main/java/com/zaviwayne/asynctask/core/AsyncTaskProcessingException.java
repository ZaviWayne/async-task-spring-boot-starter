package com.zaviwayne.asynctask.core;

/**
 * 异步任务处理异常。
 *
 * @since 2026-08-26
 */
public class AsyncTaskProcessingException extends AsyncTaskException {
    /**
     * 创建带原始异常的异步任务处理异常。
     *
     * @param message 异常信息
     * @param cause   原始异常
     */
    public AsyncTaskProcessingException(String message, Throwable cause) {
        super(message, cause);
    }
}
