package com.zaviwayne.asynctask.jdbc;

import com.zaviwayne.asynctask.core.AsyncTaskAdmin;
import com.zaviwayne.asynctask.core.AsyncTaskInfo;
import com.zaviwayne.asynctask.core.AsyncTaskRequeueResult;
import com.zaviwayne.asynctask.core.AsyncTaskStatistics;

import java.time.Clock;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

/**
 * JDBC 异步任务查询与管理入口。
 *
 * @since 2026-08-27
 */
public final class JdbcAsyncTaskAdmin implements AsyncTaskAdmin {
    /**
     * 单页最大任务数量。
     */
    private static final int MAX_PAGE_SIZE = 500;

    /**
     * JDBC 状态存储。
     */
    private final JdbcTaskStore taskStore;

    /**
     * 系统时钟。
     */
    private final Clock clock;

    /**
     * 创建 JDBC 异步任务查询与管理入口。
     *
     * @param taskStore JDBC 状态存储
     * @param clock     系统时钟
     */
    public JdbcAsyncTaskAdmin(JdbcTaskStore taskStore, Clock clock) {
        this.taskStore = Objects.requireNonNull(taskStore, "JDBC 状态存储不能为空");
        this.clock = Objects.requireNonNull(clock, "系统时钟不能为空");
    }

    /**
     * 按任务 ID 查询任务。
     *
     * @param taskId 任务 ID
     * @return 任务信息，不存在时为空
     */
    @Override
    public Optional<AsyncTaskInfo> findByTaskId(UUID taskId) {
        return taskStore.findByTaskId(Objects.requireNonNull(taskId, "任务 ID 不能为空"));
    }

    /**
     * 按幂等键查询任务。
     *
     * @param idempotencyKey 幂等键
     * @return 任务信息，不存在时为空
     */
    @Override
    public Optional<AsyncTaskInfo> findByIdempotencyKey(String idempotencyKey) {
        return taskStore.findByIdempotencyKey(requireText(idempotencyKey, "幂等键"));
    }

    /**
     * 按业务关联信息分页查询任务。
     *
     * @param referenceType 业务关联类型
     * @param referenceId   业务关联标识
     * @param limit         每页数量
     * @param offset        偏移量
     * @return 当前页任务信息
     */
    @Override
    public List<AsyncTaskInfo> findByReference(String referenceType,
                                               String referenceId,
                                               int limit,
                                               long offset) {
        if (limit <= 0 || limit > MAX_PAGE_SIZE) {
            throw new IllegalArgumentException("每页任务数量必须在 1 到 " + MAX_PAGE_SIZE + " 之间");
        }
        if (offset < 0) {
            throw new IllegalArgumentException("分页偏移量不能小于 0");
        }
        return taskStore.findByReference(
                requireText(referenceType, "业务关联类型"),
                requireText(referenceId, "业务关联标识"),
                limit,
                offset);
    }

    /**
     * 查询任务队列统计。
     *
     * @return 任务队列统计
     */
    @Override
    public AsyncTaskStatistics statistics() {
        return taskStore.statistics();
    }

    /**
     * 将执行成功或死信终态任务重新放入待投递队列。
     *
     * @param taskId 任务 ID
     * @return 重新入队结果
     */
    @Override
    public AsyncTaskRequeueResult requeue(UUID taskId) {
        return taskStore.requeue(
                Objects.requireNonNull(taskId, "任务 ID 不能为空"), clock.instant());
    }

    private static String requireText(String value, String fieldName) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(fieldName + "不能为空");
        }
        return value.trim();
    }
}
