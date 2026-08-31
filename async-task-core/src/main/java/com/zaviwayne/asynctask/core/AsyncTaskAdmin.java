package com.zaviwayne.asynctask.core;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * 异步任务查询与管理入口。
 *
 * @since 2026-08-27
 */
public interface AsyncTaskAdmin {
    /**
     * 按任务 ID 查询任务。
     *
     * @param taskId 任务 ID
     * @return 任务信息，不存在时为空
     */
    Optional<AsyncTaskInfo> findByTaskId(UUID taskId);

    /**
     * 按幂等键查询任务。
     *
     * @param idempotencyKey 幂等键
     * @return 任务信息，不存在时为空
     */
    Optional<AsyncTaskInfo> findByIdempotencyKey(String idempotencyKey);

    /**
     * 按业务关联信息分页查询任务，结果按创建时间和任务 ID 倒序排列。
     *
     * @param referenceType 业务关联类型
     * @param referenceId   业务关联标识
     * @param limit         每页数量
     * @param offset        偏移量
     * @return 当前页任务信息
     */
    List<AsyncTaskInfo> findByReference(String referenceType, String referenceId, int limit, long offset);

    /**
     * 查询任务队列统计。
     *
     * @return 任务队列统计
     */
    AsyncTaskStatistics statistics();

    /**
     * 将执行成功或死信终态任务重新放入待投递队列。
     *
     * @param taskId 任务 ID
     * @return 重新入队结果
     */
    AsyncTaskRequeueResult requeue(UUID taskId);
}
