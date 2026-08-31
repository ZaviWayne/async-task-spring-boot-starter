package com.zaviwayne.asynctask.core;

/**
 * 异步任务幂等冲突异常。
 *
 * @since 2026-08-26
 */
public class DuplicateAsyncTaskException extends AsyncTaskException {
    /**
     * 创建异步任务幂等冲突异常。
     *
     * @param message 异常消息
     */
    public DuplicateAsyncTaskException(String message) {
        super(message);
    }
}
