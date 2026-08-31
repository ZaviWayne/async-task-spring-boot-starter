package com.zaviwayne.asynctask.autoconfigure.retention;

import com.zaviwayne.asynctask.autoconfigure.jdbc.AsyncTaskJdbcAutoConfiguration;
import com.zaviwayne.asynctask.autoconfigure.properties.AsyncTaskProperties;
import com.zaviwayne.asynctask.jdbc.JdbcTaskStore;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;

import java.time.Clock;

/**
 * 异步任务终态数据保留自动配置。
 *
 * @since 2026-08-27
 */
@AutoConfiguration(after = AsyncTaskJdbcAutoConfiguration.class)
@ConditionalOnBean(JdbcTaskStore.class)
@ConditionalOnProperty(prefix = "async-task", name = "enabled", havingValue = "true", matchIfMissing = true)
@ConditionalOnProperty(prefix = "async-task.retention", name = "enabled", havingValue = "true")
public class AsyncTaskRetentionAutoConfiguration {
    /**
     * 创建终态任务定时清理作业。
     *
     * @param taskStore  JDBC 任务状态存储
     * @param clock      UTC 时钟
     * @param properties starter 配置
     * @return 终态任务定时清理作业
     */
    @Bean(destroyMethod = "close")
    @ConditionalOnMissingBean
    public AsyncTaskCleanupJob asyncTaskCleanupJob(JdbcTaskStore taskStore,
                                                   Clock clock,
                                                   AsyncTaskProperties properties) {
        return new AsyncTaskCleanupJob(taskStore, clock, properties.retention());
    }
}
