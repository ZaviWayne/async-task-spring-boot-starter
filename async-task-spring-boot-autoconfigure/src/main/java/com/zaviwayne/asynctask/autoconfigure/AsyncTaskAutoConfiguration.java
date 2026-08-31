package com.zaviwayne.asynctask.autoconfigure;

import com.zaviwayne.asynctask.autoconfigure.properties.AsyncTaskProperties;
import com.zaviwayne.asynctask.autoconfigure.serialization.JacksonTaskPayloadSerializer;
import com.zaviwayne.asynctask.core.AsyncTaskContentLimits;
import com.zaviwayne.asynctask.core.AsyncTaskHandler;
import com.zaviwayne.asynctask.core.TaskPayloadSerializer;
import com.zaviwayne.asynctask.jdbc.AsyncTaskHandlerRegistry;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import tools.jackson.databind.ObjectMapper;

import java.time.Clock;
import java.util.List;

/**
 * 异步任务 Spring Boot 基础自动配置。
 *
 * @since 2026-08-26
 */
@AutoConfiguration(afterName = "org.springframework.boot.jackson.autoconfigure.JacksonAutoConfiguration")
@EnableConfigurationProperties(AsyncTaskProperties.class)
@ConditionalOnProperty(prefix = "async-task", name = "enabled", havingValue = "true", matchIfMissing = true)
public class AsyncTaskAutoConfiguration {
    /**
     * 创建异步任务内容大小限制。
     *
     * @param properties starter 配置
     * @return 异步任务内容大小限制
     */
    @Bean
    @ConditionalOnMissingBean
    public AsyncTaskContentLimits asyncTaskContentLimits(AsyncTaskProperties properties) {
        return new AsyncTaskContentLimits(
                properties.outbox().maxEnvelopeBytes(),
                properties.outbox().maxProgressBytes());
    }

    /**
     * 创建系统 UTC 时钟。
     *
     * @return UTC 时钟
     */
    @Bean
    @ConditionalOnMissingBean
    public Clock asyncTaskClock() {
        return Clock.systemUTC();
    }

    /**
     * 创建 Jackson 任务载荷序列化器。
     *
     * @param objectMapper Jackson 对象映射器
     * @return 任务载荷序列化器
     */
    @Bean
    @ConditionalOnMissingBean(TaskPayloadSerializer.class)
    public TaskPayloadSerializer asyncTaskPayloadSerializer(ObjectMapper objectMapper) {
        return new JacksonTaskPayloadSerializer(objectMapper);
    }

    /**
     * 创建业务任务处理器注册表。
     *
     * @param handlers 业务任务处理器
     * @return 任务处理器注册表
     */
    @Bean
    @ConditionalOnMissingBean
    public AsyncTaskHandlerRegistry asyncTaskHandlerRegistry(
            ObjectProvider<AsyncTaskHandler<?>> handlers) {
        List<AsyncTaskHandler<?>> handlerList = handlers.orderedStream().toList();
        return new AsyncTaskHandlerRegistry(handlerList);
    }
}
