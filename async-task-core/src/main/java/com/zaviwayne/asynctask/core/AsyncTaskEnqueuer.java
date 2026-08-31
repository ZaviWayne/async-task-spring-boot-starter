package com.zaviwayne.asynctask.core;

import java.util.UUID;

/**
 * 异步任务入队器。
 *
 * @since 2026-08-26
 */
public interface AsyncTaskEnqueuer {
    /**
     * 在当前业务事务中持久化异步任务。
     *
     * @param request 异步任务请求
     * @param <T>     载荷类型
     * @return 新任务或已有幂等任务的 ID
     * @throws IllegalStateException       当前不存在活动事务时抛出
     * @throws DuplicateAsyncTaskException 相同幂等键对应不同任务内容时抛出
     */
    <T> UUID enqueue(AsyncTaskRequest<T> request);
}
