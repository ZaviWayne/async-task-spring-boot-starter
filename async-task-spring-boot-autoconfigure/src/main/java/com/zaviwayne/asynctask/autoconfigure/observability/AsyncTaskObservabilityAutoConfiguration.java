package com.zaviwayne.asynctask.autoconfigure.observability;

import com.zaviwayne.asynctask.autoconfigure.AsyncTaskAutoConfiguration;
import com.zaviwayne.asynctask.autoconfigure.jdbc.AsyncTaskJdbcAutoConfiguration;
import com.zaviwayne.asynctask.autoconfigure.properties.AsyncTaskProperties;
import com.zaviwayne.asynctask.core.AsyncTaskAdmin;
import com.zaviwayne.asynctask.core.AsyncTaskObserver;
import com.zaviwayne.asynctask.core.NoOpAsyncTaskObserver;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.MeterBinder;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;

import java.time.Clock;

/**
 * 异步任务可观测性自动配置。
 *
 * @since 2026-08-27
 */
@AutoConfiguration(after = {AsyncTaskAutoConfiguration.class, AsyncTaskJdbcAutoConfiguration.class})
@ConditionalOnClass(MeterRegistry.class)
@ConditionalOnProperty(prefix = "async-task", name = "enabled", havingValue = "true", matchIfMissing = true)
public class AsyncTaskObservabilityAutoConfiguration {
    /**
     * 创建任务生命周期观察器。
     *
     * @param meterRegistries 可选的 Micrometer 指标注册表
     * @return 任务生命周期观察器
     */
    @Bean
    @ConditionalOnMissingBean
    public AsyncTaskObserver asyncTaskObserver(ObjectProvider<MeterRegistry> meterRegistries) {
        MeterRegistry meterRegistry = meterRegistries.getIfAvailable();
        return meterRegistry == null
                ? NoOpAsyncTaskObserver.INSTANCE
                : new MicrometerAsyncTaskObserver(meterRegistry);
    }

    /**
     * 创建异步任务队列指标绑定器。
     *
     * @param taskAdmin  异步任务管理门面
     * @param clock      UTC 时钟
     * @param properties starter 配置
     * @return Micrometer 指标绑定器
     */
    @Bean
    @ConditionalOnBean({MeterRegistry.class, AsyncTaskAdmin.class})
    @ConditionalOnMissingBean(name = "asyncTaskMetrics")
    public MeterBinder asyncTaskMetrics(AsyncTaskAdmin taskAdmin,
                                        Clock clock,
                                        AsyncTaskProperties properties) {
        return new AsyncTaskMetrics(
                taskAdmin, clock, properties.observability().statisticsCacheDuration());
    }
}
