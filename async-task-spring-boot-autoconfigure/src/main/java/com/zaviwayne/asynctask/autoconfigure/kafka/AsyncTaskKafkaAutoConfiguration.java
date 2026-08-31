package com.zaviwayne.asynctask.autoconfigure.kafka;

import com.zaviwayne.asynctask.autoconfigure.jdbc.AsyncTaskJdbcAutoConfiguration;
import com.zaviwayne.asynctask.autoconfigure.observability.AsyncTaskObservabilityAutoConfiguration;
import com.zaviwayne.asynctask.autoconfigure.properties.AsyncTaskOutboxProperties;
import com.zaviwayne.asynctask.autoconfigure.properties.AsyncTaskProperties;
import com.zaviwayne.asynctask.autoconfigure.properties.AsyncTaskTopicBinding;
import com.zaviwayne.asynctask.core.AsyncTaskContentLimits;
import com.zaviwayne.asynctask.core.AsyncTaskObserver;
import com.zaviwayne.asynctask.core.AsyncTaskTransport;
import com.zaviwayne.asynctask.core.TaskPayloadSerializer;
import com.zaviwayne.asynctask.jdbc.AsyncTaskDispatcher;
import com.zaviwayne.asynctask.jdbc.AsyncTaskProcessor;
import com.zaviwayne.asynctask.jdbc.ExponentialBackoffPolicy;
import com.zaviwayne.asynctask.jdbc.JdbcTaskStore;
import com.zaviwayne.asynctask.kafka.AsyncTaskKafkaMessageHandler;
import com.zaviwayne.asynctask.kafka.KafkaAsyncTaskTransport;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaOperations;

import java.time.Clock;
import java.util.ArrayList;
import java.util.List;

/**
 * 异步任务 Kafka 自动配置。
 *
 * @since 2026-08-27
 */
@AutoConfiguration(after = {
        AsyncTaskJdbcAutoConfiguration.class,
        AsyncTaskObservabilityAutoConfiguration.class
}, afterName = "org.springframework.boot.kafka.autoconfigure.KafkaAutoConfiguration")
@ConditionalOnClass({KafkaOperations.class, KafkaAsyncTaskTransport.class})
@ConditionalOnBean({KafkaOperations.class, ConsumerFactory.class, JdbcTaskStore.class})
@ConditionalOnProperty(prefix = "async-task", name = "enabled", havingValue = "true", matchIfMissing = true)
@ConditionalOnProperty(
        prefix = "async-task.kafka", name = "enabled", havingValue = "true", matchIfMissing = true)
public class AsyncTaskKafkaAutoConfiguration {
    /**
     * 每个业务主题绑定自动声明的主题数量。
     */
    private static final int TOPICS_PER_BINDING = 3;

    /**
     * 创建 Kafka 任务传输通道。
     *
     * @param kafkaOperations Kafka 操作接口
     * @param serializer      任务载荷序列化器
     * @param properties      starter 配置
     * @param contentLimits   任务内容大小限制
     * @return Kafka 任务传输通道
     */
    @Bean
    @ConditionalOnMissingBean(AsyncTaskTransport.class)
    public AsyncTaskTransport asyncTaskTransport(KafkaOperations<Object, Object> kafkaOperations,
                                                 TaskPayloadSerializer serializer,
                                                 AsyncTaskProperties properties,
                                                 AsyncTaskContentLimits contentLimits) {
        return new KafkaAsyncTaskTransport(
                kafkaOperations,
                serializer,
                properties.kafka().sendTimeout(),
                properties.kafka().transactionEnabled(),
                contentLimits);
    }

    /**
     * 声明配置的 Kafka 业务主题和死信主题。
     *
     * @param properties starter 配置
     * @return Kafka 主题声明
     */
    @Bean
    @ConditionalOnProperty(
            prefix = "async-task.kafka", name = "auto-create-topics", havingValue = "true")
    public KafkaAdmin.NewTopics asyncTaskKafkaTopics(AsyncTaskProperties properties) {
        List<NewTopic> topics = new ArrayList<>(
                properties.kafka().bindings().size() * TOPICS_PER_BINDING);
        for (AsyncTaskTopicBinding binding : properties.kafka().bindings()) {
            topics.add(topic(binding.topic(), binding));
            topics.add(topic(binding.deadLetterTopic(), binding));
            topics.add(topic(binding.parkingTopic(), binding));
        }
        return new KafkaAdmin.NewTopics(topics.toArray(NewTopic[]::new));
    }

    /**
     * 创建 Kafka 消息入口。
     *
     * @param serializer    任务载荷序列化器
     * @param taskProcessor 异步任务处理器
     * @param contentLimits 任务内容大小限制
     * @return Kafka 消息入口
     */
    @Bean
    @ConditionalOnMissingBean
    public AsyncTaskKafkaMessageHandler asyncTaskKafkaMessageHandler(
            TaskPayloadSerializer serializer,
            AsyncTaskProcessor taskProcessor,
            AsyncTaskContentLimits contentLimits) {
        return new AsyncTaskKafkaMessageHandler(serializer, taskProcessor, contentLimits);
    }

    /**
     * 创建 Kafka 消费监听容器管理器。
     *
     * @param consumerFactory Kafka 消费者工厂
     * @param kafkaOperations Kafka 操作接口
     * @param messageHandler  Kafka 消息入口
     * @param properties      starter 配置
     * @return Kafka 消费监听容器管理器
     */
    @Bean
    @ConditionalOnMissingBean
    public AsyncTaskKafkaContainers asyncTaskKafkaContainers(
            ConsumerFactory<Object, Object> consumerFactory,
            KafkaOperations<Object, Object> kafkaOperations,
            AsyncTaskKafkaMessageHandler messageHandler,
            AsyncTaskProperties properties) {
        return new AsyncTaskKafkaContainers(
                consumerFactory, kafkaOperations, messageHandler, properties.kafka());
    }

    /**
     * 创建 outbox 投递器。
     *
     * @param taskStore  JDBC 状态存储
     * @param transport  Kafka 任务传输通道
     * @param clock      UTC 时钟
     * @param properties starter 配置
     * @param observer   任务生命周期观察器
     * @return Outbox 投递器
     */
    @Bean
    @ConditionalOnMissingBean
    public AsyncTaskDispatcher asyncTaskDispatcher(JdbcTaskStore taskStore,
                                                   AsyncTaskTransport transport,
                                                   Clock clock,
                                                   AsyncTaskProperties properties,
                                                   AsyncTaskObserver observer) {
        AsyncTaskOutboxProperties outbox = properties.outbox();
        ExponentialBackoffPolicy retryPolicy = new ExponentialBackoffPolicy(
                outbox.maxAttempts(), outbox.initialBackoff(), outbox.maxBackoff());
        return new AsyncTaskDispatcher(
                taskStore, transport, retryPolicy, clock, outbox.batchSize(),
                outbox.leaseDuration(), observer);
    }

    /**
     * 创建 outbox 定时轮询作业。
     *
     * @param dispatcher Outbox 投递器
     * @param properties starter 配置
     * @return Outbox 定时轮询作业
     */
    @Bean(destroyMethod = "close")
    @ConditionalOnProperty(
            prefix = "async-task.outbox", name = "enabled", havingValue = "true", matchIfMissing = true)
    @ConditionalOnMissingBean
    public AsyncTaskPollingJob asyncTaskPollingJob(AsyncTaskDispatcher dispatcher,
                                                   AsyncTaskProperties properties) {
        return new AsyncTaskPollingJob(dispatcher, properties.outbox().pollInterval());
    }

    private static NewTopic topic(String name, AsyncTaskTopicBinding binding) {
        return TopicBuilder.name(name)
                .partitions(binding.partitions())
                .replicas(binding.replicas())
                .build();
    }
}
