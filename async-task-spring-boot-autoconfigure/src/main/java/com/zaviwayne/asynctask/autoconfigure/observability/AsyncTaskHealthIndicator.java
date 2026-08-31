package com.zaviwayne.asynctask.autoconfigure.observability;

import com.zaviwayne.asynctask.jdbc.JdbcTaskStore;
import org.springframework.boot.health.contributor.Health;
import org.springframework.boot.health.contributor.HealthIndicator;

import java.util.Objects;

/**
 * 异步任务健康检查。
 *
 * @since 2026-08-27
 */
public final class AsyncTaskHealthIndicator implements HealthIndicator {
    /**
     * JDBC 状态存储。
     */
    private final JdbcTaskStore taskStore;

    /**
     * 创建异步任务健康检查。
     *
     * @param taskStore JDBC 状态存储
     */
    public AsyncTaskHealthIndicator(JdbcTaskStore taskStore) {
        this.taskStore = Objects.requireNonNull(taskStore, "JDBC 状态存储不能为空");
    }

    /**
     * 检查异步任务表是否可以访问。
     *
     * @return 异步任务存储健康状态
     */
    @Override
    public Health health() {
        try {
            taskStore.checkHealth();
            return Health.up().build();
        } catch (RuntimeException exception) {
            return Health.down(exception).build();
        }
    }
}
