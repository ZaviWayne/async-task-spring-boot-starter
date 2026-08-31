package com.zaviwayne.asynctask.autoconfigure.observability;

import com.zaviwayne.asynctask.autoconfigure.jdbc.AsyncTaskJdbcAutoConfiguration;
import com.zaviwayne.asynctask.jdbc.JdbcTaskStore;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.health.contributor.HealthIndicator;
import org.springframework.context.annotation.Bean;

/**
 * 异步任务健康检查自动配置。
 *
 * @since 2026-08-27
 */
@AutoConfiguration(after = AsyncTaskJdbcAutoConfiguration.class)
@ConditionalOnClass(HealthIndicator.class)
@ConditionalOnBean(JdbcTaskStore.class)
@ConditionalOnProperty(prefix = "async-task", name = "enabled", havingValue = "true", matchIfMissing = true)
public class AsyncTaskHealthAutoConfiguration {
    /**
     * 创建异步任务健康检查。
     *
     * @param taskStore JDBC 状态存储
     * @return 异步任务健康检查
     */
    @Bean
    @ConditionalOnMissingBean(name = "asyncTaskHealthIndicator")
    public HealthIndicator asyncTaskHealthIndicator(JdbcTaskStore taskStore) {
        return new AsyncTaskHealthIndicator(taskStore);
    }
}
