package com.zaviwayne.asynctask.core;

/**
 * 异步任务处理器不存在异常。
 *
 * @since 2026-08-26
 */
public class NoAsyncTaskHandlerException extends AsyncTaskException {
    /**
     * 创建异步任务处理器不存在异常。
     *
     * @param message 异常消息
     */
    public NoAsyncTaskHandlerException(String message) {
        super(message);
    }
}
