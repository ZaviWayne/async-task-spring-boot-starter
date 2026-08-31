package com.zaviwayne.asynctask.autoconfigure.kafka;

import com.zaviwayne.asynctask.autoconfigure.properties.AsyncTaskKafkaProperties;
import com.zaviwayne.asynctask.autoconfigure.properties.AsyncTaskTopicBinding;
import com.zaviwayne.asynctask.core.DuplicateAsyncTaskException;
import com.zaviwayne.asynctask.core.InvalidAsyncTaskMessageException;
import com.zaviwayne.asynctask.core.NoAsyncTaskHandlerException;
import com.zaviwayne.asynctask.core.TaskExecutionInProgressException;
import com.zaviwayne.asynctask.kafka.AsyncTaskKafkaMessageHandler;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.springframework.context.SmartLifecycle;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.listener.*;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.util.backoff.FixedBackOff;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * 异步任务 Kafka 消费容器管理器。
 *
 * @since 2026-08-26
 */
public final class AsyncTaskKafkaContainers implements SmartLifecycle {
    /**
     * Kafka 已提交消息隔离级别。
     */
    private static final String READ_COMMITTED_ISOLATION_LEVEL = "read_committed";

    /**
     * Kafka 消费容器生命周期阶段。
     */
    private static final int LIFECYCLE_PHASE = 0;

    /**
     * 普通消息消费容器列表。
     */
    private final List<ConcurrentMessageListenerContainer<Object, Object>> taskContainers;

    /**
     * 死信消息消费容器列表。
     */
    private final List<ConcurrentMessageListenerContainer<Object, Object>> deadLetterContainers;

    /**
     * 生命周期运行状态。
     */
    private volatile boolean running;

    /**
     * 创建异步任务 Kafka 消费容器管理器。
     *
     * @param consumerFactory Kafka 消费者工厂
     * @param kafkaOperations Kafka 操作接口
     * @param messageHandler  Kafka 消息入口
     * @param properties      Kafka 配置
     */
    public AsyncTaskKafkaContainers(ConsumerFactory<Object, Object> consumerFactory,
                                    KafkaOperations<Object, Object> kafkaOperations,
                                    AsyncTaskKafkaMessageHandler messageHandler,
                                    AsyncTaskKafkaProperties properties) {
        Objects.requireNonNull(consumerFactory, "Kafka 消费者工厂不能为空");
        Objects.requireNonNull(kafkaOperations, "Kafka 操作接口不能为空");
        Objects.requireNonNull(messageHandler, "Kafka 消息入口不能为空");
        Objects.requireNonNull(properties, "Kafka 配置不能为空");
        List<AsyncTaskTopicBinding> bindings = properties.bindings();
        if (bindings.isEmpty()) {
            this.taskContainers = List.of();
            this.deadLetterContainers = List.of();
            return;
        }
        validateTransactionConfiguration(consumerFactory, kafkaOperations, properties);
        List<ConcurrentMessageListenerContainer<Object, Object>> createdTaskContainers =
                new ArrayList<>(bindings.size());
        List<ConcurrentMessageListenerContainer<Object, Object>> createdDeadLetterContainers =
                new ArrayList<>(bindings.size());
        for (int index = 0; index < bindings.size(); index++) {
            AsyncTaskTopicBinding binding = bindings.get(index);
            createdTaskContainers.add(createTaskContainer(
                    consumerFactory, kafkaOperations, messageHandler, properties, binding, index));
            createdDeadLetterContainers.add(createDeadLetterContainer(
                    consumerFactory, kafkaOperations, messageHandler, properties, binding, index));
        }
        this.taskContainers = List.copyOf(createdTaskContainers);
        this.deadLetterContainers = List.copyOf(createdDeadLetterContainers);
    }

    /**
     * 启动普通消息与死信消息消费容器。
     */
    @Override
    public void start() {
        deadLetterContainers.forEach(ConcurrentMessageListenerContainer::start);
        taskContainers.forEach(ConcurrentMessageListenerContainer::start);
        running = true;
    }

    /**
     * 停止普通消息与死信消息消费容器。
     */
    @Override
    public void stop() {
        taskContainers.forEach(ConcurrentMessageListenerContainer::stop);
        deadLetterContainers.forEach(ConcurrentMessageListenerContainer::stop);
        running = false;
    }

    /**
     * 判断消费容器管理器是否正在运行。
     *
     * @return 正在运行时返回 true
     */
    @Override
    public boolean isRunning() {
        return running;
    }

    /**
     * 获取 Kafka 消费容器的生命周期阶段。
     *
     * @return 生命周期阶段
     */
    @Override
    public int getPhase() {
        return LIFECYCLE_PHASE;
    }

    private static ConcurrentMessageListenerContainer<Object, Object> createTaskContainer(
            ConsumerFactory<Object, Object> consumerFactory,
            KafkaOperations<Object, Object> kafkaOperations,
            AsyncTaskKafkaMessageHandler messageHandler,
            AsyncTaskKafkaProperties properties,
            AsyncTaskTopicBinding binding,
            int index) {
        ContainerProperties containerProperties = containerProperties(
                List.of(binding.topic()), binding.consumerGroup());
        containerProperties.setMessageListener((MessageListener<Object, Object>) record ->
                messageHandler.handle(record.topic(), record.key(), requireStringValue(record)));
        ConcurrentMessageListenerContainer<Object, Object> container =
                new ConcurrentMessageListenerContainer<>(consumerFactory, containerProperties);
        container.setBeanName("asyncTaskKafkaListenerContainer-" + index);
        container.setAutoStartup(false);
        container.setConcurrency(binding.concurrency());
        container.setCommonErrorHandler(errorHandler(kafkaOperations, properties, binding));
        return container;
    }

    private static ConcurrentMessageListenerContainer<Object, Object> createDeadLetterContainer(
            ConsumerFactory<Object, Object> consumerFactory,
            KafkaOperations<Object, Object> kafkaOperations,
            AsyncTaskKafkaMessageHandler messageHandler,
            AsyncTaskKafkaProperties properties,
            AsyncTaskTopicBinding binding,
            int index) {
        ContainerProperties containerProperties = containerProperties(
                List.of(binding.deadLetterTopic()), binding.consumerGroup() + ".dlt");
        containerProperties.setMessageListener((MessageListener<Object, Object>) record ->
                handleDeadLetterRecord(record, messageHandler, binding.topic()));
        ConcurrentMessageListenerContainer<Object, Object> container =
                new ConcurrentMessageListenerContainer<>(consumerFactory, containerProperties);
        container.setBeanName("asyncTaskKafkaDeadLetterContainer-" + index);
        container.setAutoStartup(false);
        container.setConcurrency(binding.concurrency());
        container.setCommonErrorHandler(deadLetterErrorHandler(kafkaOperations, properties, binding));
        return container;
    }

    private static ContainerProperties containerProperties(List<String> topics, String consumerGroup) {
        ContainerProperties containerProperties = new ContainerProperties(topics.toArray(String[]::new));
        containerProperties.setGroupId(consumerGroup);
        containerProperties.setAckMode(ContainerProperties.AckMode.RECORD);
        containerProperties.setMissingTopicsFatal(false);
        return containerProperties;
    }

    private static DefaultErrorHandler errorHandler(KafkaOperations<Object, Object> kafkaOperations,
                                                    AsyncTaskKafkaProperties properties,
                                                    AsyncTaskTopicBinding binding) {
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(
                producerRecord -> kafkaOperations,
                properties.transactionEnabled(),
                (record, exception) -> new TopicPartition(
                        binding.deadLetterTopic(), record.partition()));
        recoverer.setFailIfSendResultIsError(true);
        recoverer.setWaitForSendResultTimeout(properties.sendTimeout());
        FixedBackOff businessFailureBackOff = new FixedBackOff(
                properties.retryInterval().toMillis(), properties.maxRetries());
        FixedBackOff activeLeaseBackOff = new FixedBackOff(
                properties.leaseRetryInterval().toMillis(), FixedBackOff.UNLIMITED_ATTEMPTS);
        DefaultErrorHandler errorHandler = new DefaultErrorHandler(recoverer, businessFailureBackOff);
        errorHandler.setBackOffFunction((record, exception) ->
                hasCause(exception, TaskExecutionInProgressException.class)
                        ? activeLeaseBackOff
                        : businessFailureBackOff);
        errorHandler.addNotRetryableExceptions(
                InvalidAsyncTaskMessageException.class,
                DuplicateAsyncTaskException.class,
                NoAsyncTaskHandlerException.class);
        errorHandler.setResetStateOnExceptionChange(true);
        return errorHandler;
    }

    private static DefaultErrorHandler deadLetterErrorHandler(
            KafkaOperations<Object, Object> kafkaOperations,
            AsyncTaskKafkaProperties properties,
            AsyncTaskTopicBinding binding) {
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(
                producerRecord -> kafkaOperations,
                properties.transactionEnabled(),
                (record, exception) -> new TopicPartition(binding.parkingTopic(), record.partition()));
        recoverer.setFailIfSendResultIsError(true);
        recoverer.setWaitForSendResultTimeout(properties.sendTimeout());
        FixedBackOff retryBackOff = new FixedBackOff(
                properties.retryInterval().toMillis(), properties.deadLetterMaxRetries());
        DefaultErrorHandler errorHandler = new DefaultErrorHandler(recoverer, retryBackOff);
        errorHandler.addNotRetryableExceptions(
                InvalidAsyncTaskMessageException.class,
                DuplicateAsyncTaskException.class,
                NoAsyncTaskHandlerException.class);
        return errorHandler;
    }

    private static void validateTransactionConfiguration(
            ConsumerFactory<Object, Object> consumerFactory,
            KafkaOperations<Object, Object> kafkaOperations,
            AsyncTaskKafkaProperties properties) {
        if (!properties.transactionEnabled()) {
            return;
        }
        if (!kafkaOperations.isTransactional()) {
            throw new IllegalArgumentException("启用 Kafka 事务发送时必须配置事务型 KafkaTemplate");
        }
        Object isolationLevel = consumerFactory.getConfigurationProperties()
                .get(ConsumerConfig.ISOLATION_LEVEL_CONFIG);
        boolean readCommitted = isolationLevel != null
                && READ_COMMITTED_ISOLATION_LEVEL.equalsIgnoreCase(isolationLevel.toString());
        if (!readCommitted) {
            throw new IllegalArgumentException("启用 Kafka 事务发送时消费者隔离级别必须为 read_committed");
        }
    }

    private static boolean hasCause(Throwable throwable, Class<? extends Throwable> causeType) {
        Throwable current = throwable;
        while (current != null) {
            if (causeType.isInstance(current)) {
                return true;
            }
            Throwable cause = current.getCause();
            if (cause == current) {
                return false;
            }
            current = cause;
        }
        return false;
    }

    private static String requireStringValue(ConsumerRecord<Object, Object> record) {
        if (record.value() instanceof String value) {
            return value;
        }
        throw new InvalidAsyncTaskMessageException(
                "Kafka 异步任务消息必须使用字符串反序列化器");
    }

    private static String deadLetterReason(ConsumerRecord<Object, Object> record) {
        Header header = record.headers().lastHeader(KafkaHeaders.DLT_EXCEPTION_MESSAGE);
        if (header == null || header.value() == null) {
            return "Kafka 消费重试次数已耗尽";
        }
        return new String(header.value(), StandardCharsets.UTF_8);
    }

    private static void handleDeadLetterRecord(ConsumerRecord<Object, Object> record,
                                               AsyncTaskKafkaMessageHandler messageHandler,
                                               String expectedDestination) {
        messageHandler.handleDeadLetter(
                expectedDestination, record.key(), requireStringValue(record), deadLetterReason(record));
    }
}
