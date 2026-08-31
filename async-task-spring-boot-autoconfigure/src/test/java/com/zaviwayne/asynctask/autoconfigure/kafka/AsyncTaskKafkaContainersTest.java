package com.zaviwayne.asynctask.autoconfigure.kafka;

import com.zaviwayne.asynctask.autoconfigure.properties.AsyncTaskKafkaProperties;

import com.zaviwayne.asynctask.kafka.AsyncTaskKafkaMessageHandler;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaOperations;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class AsyncTaskKafkaContainersTest {
    @Test
    void shouldRequireReadCommittedConsumersForKafkaTransactions() {
        ConsumerFactory<Object, Object> consumerFactory = mock(ConsumerFactory.class);
        KafkaOperations<Object, Object> kafkaOperations = mock(KafkaOperations.class);
        when(kafkaOperations.isTransactional()).thenReturn(true);
        when(consumerFactory.getConfigurationProperties()).thenReturn(Map.of());

        assertThatThrownBy(() -> new AsyncTaskKafkaContainers(
                consumerFactory,
                kafkaOperations,
                mock(AsyncTaskKafkaMessageHandler.class),
                transactionalProperties()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("启用 Kafka 事务发送时消费者隔离级别必须为 read_committed");
    }

    @Test
    void shouldAcceptReadCommittedConsumersForKafkaTransactions() {
        ConsumerFactory<Object, Object> consumerFactory = mock(ConsumerFactory.class);
        KafkaOperations<Object, Object> kafkaOperations = mock(KafkaOperations.class);
        when(kafkaOperations.isTransactional()).thenReturn(true);
        when(consumerFactory.getConfigurationProperties()).thenReturn(Map.of(
                ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed"));

        AsyncTaskKafkaContainers containers = new AsyncTaskKafkaContainers(
                consumerFactory,
                kafkaOperations,
                mock(AsyncTaskKafkaMessageHandler.class),
                transactionalProperties());

        containers.stop();
    }

    private static AsyncTaskKafkaProperties transactionalProperties() {
        return new AsyncTaskKafkaProperties(
                true,
                false,
                true,
                List.of("task-events"),
                List.of(),
                "task-workers",
                1,
                3,
                1,
                Duration.ofSeconds(5),
                Duration.ofSeconds(1),
                3,
                Duration.ofSeconds(1),
                ".DLT");
    }
}
