package com.zaviwayne.asynctask.autoconfigure.kafka;

import com.zaviwayne.asynctask.autoconfigure.properties.AsyncTaskKafkaProperties;
import com.zaviwayne.asynctask.core.AsyncTaskProcessingException;
import com.zaviwayne.asynctask.core.InvalidAsyncTaskMessageException;
import com.zaviwayne.asynctask.core.TaskExecutionInProgressException;
import com.zaviwayne.asynctask.kafka.AsyncTaskKafkaMessageHandler;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.EmbeddedKafkaKraftBroker;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class AsyncTaskKafkaContainersIntegrationTest {
    private static final String LEASE_TOPIC = "async-task-lease-test";

    private static final String FAILURE_TOPIC = "async-task-failure-test";

    private static final String INVALID_TOPIC = "async-task-invalid-test";

    private static final String DEAD_LETTER_RETRY_TOPIC = "async-task-dead-letter-retry-test";

    private static final String PARKING_TOPIC = "async-task-parking-test";

    private static final String DEAD_LETTER_SUFFIX = ".DLT";

    private static final String PARKING_SUFFIX = ".PARKING";

    private static final String ENVELOPE_JSON = "{\"taskId\":\"test-task\"}";

    private static final int EXPECTED_LEASE_ATTEMPTS = 5;

    private static final int EXPECTED_BUSINESS_ATTEMPTS = 3;

    private static final int EXPECTED_DEAD_LETTER_ATTEMPTS = 3;

    private static final Duration TEST_TIMEOUT = Duration.ofSeconds(15);

    private static final EmbeddedKafkaKraftBroker BROKER = new EmbeddedKafkaKraftBroker(
            1,
            1,
            LEASE_TOPIC,
            LEASE_TOPIC + DEAD_LETTER_SUFFIX,
            LEASE_TOPIC + DEAD_LETTER_SUFFIX + PARKING_SUFFIX,
            FAILURE_TOPIC,
            FAILURE_TOPIC + DEAD_LETTER_SUFFIX,
            FAILURE_TOPIC + DEAD_LETTER_SUFFIX + PARKING_SUFFIX,
            INVALID_TOPIC,
            INVALID_TOPIC + DEAD_LETTER_SUFFIX,
            INVALID_TOPIC + DEAD_LETTER_SUFFIX + PARKING_SUFFIX,
            DEAD_LETTER_RETRY_TOPIC,
            DEAD_LETTER_RETRY_TOPIC + DEAD_LETTER_SUFFIX,
            DEAD_LETTER_RETRY_TOPIC + DEAD_LETTER_SUFFIX + PARKING_SUFFIX,
            PARKING_TOPIC,
            PARKING_TOPIC + DEAD_LETTER_SUFFIX,
            PARKING_TOPIC + DEAD_LETTER_SUFFIX + PARKING_SUFFIX);

    @BeforeAll
    static void startBroker() {
        BROKER.afterPropertiesSet();
    }

    @AfterAll
    static void stopBroker() {
        BROKER.destroy();
    }

    @Test
    void shouldRetryActiveLeaseWithoutPublishingDeadLetter() throws Exception {
        AsyncTaskKafkaMessageHandler messageHandler = mock(AsyncTaskKafkaMessageHandler.class);
        AtomicInteger attempts = new AtomicInteger();
        CountDownLatch completed = new CountDownLatch(1);
        doAnswer(invocation -> {
            int attempt = attempts.incrementAndGet();
            if (attempt < EXPECTED_LEASE_ATTEMPTS) {
                throw new TaskExecutionInProgressException("执行租约仍然有效");
            }
            completed.countDown();
            return null;
        }).when(messageHandler).handle(LEASE_TOPIC, "lease-key", ENVELOPE_JSON);

        try (KafkaClientResources clients = kafkaClients()) {
            AsyncTaskKafkaContainers containers = new AsyncTaskKafkaContainers(
                    clients.consumerFactory(),
                    clients.kafkaTemplate(),
                    messageHandler,
                    kafkaProperties(LEASE_TOPIC, 1));
            try {
                containers.start();
                clients.kafkaTemplate().send(LEASE_TOPIC, "lease-key", ENVELOPE_JSON)
                        .get(TEST_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

                assertThat(completed.await(TEST_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)).isTrue();
                assertThat(attempts).hasValue(EXPECTED_LEASE_ATTEMPTS);
                verify(messageHandler, never()).handleDeadLetter(
                        eq(LEASE_TOPIC), any(), anyString(), anyString());
            } finally {
                containers.stop();
            }
        }
    }

    @Test
    void shouldPublishBusinessFailureToDeadLetterTopicAfterRetries() throws Exception {
        AsyncTaskKafkaMessageHandler messageHandler = mock(AsyncTaskKafkaMessageHandler.class);
        AtomicInteger attempts = new AtomicInteger();
        CountDownLatch deadLetterReceived = new CountDownLatch(1);
        AtomicReference<String> deadLetterPayload = new AtomicReference<>();
        AtomicReference<String> deadLetterReason = new AtomicReference<>();
        doAnswer(invocation -> {
            attempts.incrementAndGet();
            throw new AsyncTaskProcessingException(
                    "测试业务执行失败", new IllegalStateException("测试异常"));
        }).when(messageHandler).handle(FAILURE_TOPIC, "failure-key", ENVELOPE_JSON);
        doAnswer(invocation -> {
            deadLetterPayload.set(invocation.getArgument(2, String.class));
            deadLetterReason.set(invocation.getArgument(3, String.class));
            deadLetterReceived.countDown();
            return null;
        }).when(messageHandler).handleDeadLetter(
                eq(FAILURE_TOPIC), any(), anyString(), anyString());

        try (KafkaClientResources clients = kafkaClients()) {
            AsyncTaskKafkaContainers containers = new AsyncTaskKafkaContainers(
                    clients.consumerFactory(),
                    clients.kafkaTemplate(),
                    messageHandler,
                    kafkaProperties(FAILURE_TOPIC, 2));
            try {
                containers.start();
                clients.kafkaTemplate().send(FAILURE_TOPIC, "failure-key", ENVELOPE_JSON)
                        .get(TEST_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

                assertThat(deadLetterReceived.await(TEST_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)).isTrue();
                assertThat(attempts).hasValue(EXPECTED_BUSINESS_ATTEMPTS);
                assertThat(deadLetterPayload.get()).isEqualTo(ENVELOPE_JSON);
                assertThat(deadLetterReason.get()).isNotBlank();
            } finally {
                containers.stop();
            }
        }
    }

    @Test
    void shouldPublishInvalidMessageToDeadLetterWithoutRetry() throws Exception {
        AsyncTaskKafkaMessageHandler messageHandler = mock(AsyncTaskKafkaMessageHandler.class);
        AtomicInteger attempts = new AtomicInteger();
        CountDownLatch deadLetterReceived = new CountDownLatch(1);
        doAnswer(invocation -> {
            attempts.incrementAndGet();
            throw new InvalidAsyncTaskMessageException("测试消息无效");
        }).when(messageHandler).handle(INVALID_TOPIC, "invalid-key", ENVELOPE_JSON);
        doAnswer(invocation -> {
            deadLetterReceived.countDown();
            return null;
        }).when(messageHandler).handleDeadLetter(
                eq(INVALID_TOPIC), any(), anyString(), anyString());

        try (KafkaClientResources clients = kafkaClients()) {
            AsyncTaskKafkaContainers containers = new AsyncTaskKafkaContainers(
                    clients.consumerFactory(),
                    clients.kafkaTemplate(),
                    messageHandler,
                    kafkaProperties(INVALID_TOPIC, 5));
            try {
                containers.start();
                clients.kafkaTemplate().send(INVALID_TOPIC, "invalid-key", ENVELOPE_JSON)
                        .get(TEST_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

                assertThat(deadLetterReceived.await(TEST_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)).isTrue();
                assertThat(attempts).hasValue(1);
            } finally {
                containers.stop();
            }
        }
    }

    @Test
    void shouldRetryDeadLetterUntilHandlerSucceeds() throws Exception {
        AsyncTaskKafkaMessageHandler messageHandler = mock(AsyncTaskKafkaMessageHandler.class);
        AtomicInteger businessAttempts = new AtomicInteger();
        AtomicInteger deadLetterAttempts = new AtomicInteger();
        CountDownLatch completed = new CountDownLatch(1);
        doAnswer(invocation -> {
            businessAttempts.incrementAndGet();
            throw new AsyncTaskProcessingException(
                    "测试业务执行失败", new IllegalStateException("测试异常"));
        }).when(messageHandler).handle(
                DEAD_LETTER_RETRY_TOPIC, "dead-letter-retry-key", ENVELOPE_JSON);
        doAnswer(invocation -> {
            int attempt = deadLetterAttempts.incrementAndGet();
            if (attempt < EXPECTED_DEAD_LETTER_ATTEMPTS) {
                throw new IllegalStateException("测试死信处理失败");
            }
            completed.countDown();
            return null;
        }).when(messageHandler).handleDeadLetter(
                eq(DEAD_LETTER_RETRY_TOPIC), any(), anyString(), anyString());

        try (KafkaClientResources clients = kafkaClients()) {
            AsyncTaskKafkaContainers containers = new AsyncTaskKafkaContainers(
                    clients.consumerFactory(),
                    clients.kafkaTemplate(),
                    messageHandler,
                    kafkaProperties(DEAD_LETTER_RETRY_TOPIC, 0));
            try {
                containers.start();
                clients.kafkaTemplate().send(DEAD_LETTER_RETRY_TOPIC, "dead-letter-retry-key", ENVELOPE_JSON)
                        .get(TEST_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

                assertThat(completed.await(TEST_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)).isTrue();
                assertThat(businessAttempts).hasValue(1);
                assertThat(deadLetterAttempts).hasValue(EXPECTED_DEAD_LETTER_ATTEMPTS);
            } finally {
                containers.stop();
            }
        }
    }

    @Test
    void shouldPublishDeadLetterFailureToParkingTopicAfterRetries() throws Exception {
        AsyncTaskKafkaMessageHandler messageHandler = mock(AsyncTaskKafkaMessageHandler.class);
        AtomicInteger deadLetterAttempts = new AtomicInteger();
        doAnswer(invocation -> {
            throw new AsyncTaskProcessingException(
                    "测试业务执行失败", new IllegalStateException("测试异常"));
        }).when(messageHandler).handle(PARKING_TOPIC, "parking-key", ENVELOPE_JSON);
        doAnswer(invocation -> {
            deadLetterAttempts.incrementAndGet();
            throw new IllegalStateException("测试死信处理失败");
        }).when(messageHandler).handleDeadLetter(
                eq(PARKING_TOPIC), any(), anyString(), anyString());

        try (KafkaClientResources clients = kafkaClients();
             Consumer<Object, Object> parkingConsumer = clients.consumerFactory().createConsumer(
                     "async-task-parking-verifier-" + UUID.randomUUID(), null)) {
            String parkingTopic = PARKING_TOPIC + DEAD_LETTER_SUFFIX + PARKING_SUFFIX;
            parkingConsumer.subscribe(List.of(parkingTopic));
            AsyncTaskKafkaContainers containers = new AsyncTaskKafkaContainers(
                    clients.consumerFactory(),
                    clients.kafkaTemplate(),
                    messageHandler,
                    kafkaProperties(PARKING_TOPIC, 0, 2));
            try {
                containers.start();
                clients.kafkaTemplate().send(
                                PARKING_TOPIC, "parking-key", ENVELOPE_JSON)
                        .get(TEST_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

                ConsumerRecord<Object, Object> parkingRecord = KafkaTestUtils.getSingleRecord(
                        parkingConsumer, parkingTopic, TEST_TIMEOUT);
                assertThat(parkingRecord.key()).isEqualTo("parking-key");
                assertThat(parkingRecord.value()).isEqualTo(ENVELOPE_JSON);
                assertThat(deadLetterAttempts).hasValue(3);
            } finally {
                containers.stop();
            }
        }
    }

    private static AsyncTaskKafkaProperties kafkaProperties(String topic, long maxRetries) {
        return kafkaProperties(topic, maxRetries, 10);
    }

    private static AsyncTaskKafkaProperties kafkaProperties(String topic,
                                                            long maxRetries,
                                                            long deadLetterMaxRetries) {
        return new AsyncTaskKafkaProperties(
                true,
                false,
                false,
                List.of(topic),
                List.of(),
                "async-task-test-" + UUID.randomUUID(),
                1,
                3,
                1,
                Duration.ofSeconds(5),
                Duration.ofMillis(20),
                maxRetries,
                Duration.ofMillis(20),
                DEAD_LETTER_SUFFIX,
                deadLetterMaxRetries,
                PARKING_SUFFIX);
    }

    private static KafkaClientResources kafkaClients() {
        Map<String, Object> producerProperties = KafkaTestUtils.producerProps(BROKER);
        producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        DefaultKafkaProducerFactory<Object, Object> producerFactory =
                new DefaultKafkaProducerFactory<>(producerProperties);
        KafkaTemplate<Object, Object> kafkaTemplate = new KafkaTemplate<>(producerFactory);

        Map<String, Object> consumerProperties = KafkaTestUtils.consumerProps(
                BROKER, "async-task-test-consumer", false);
        consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProperties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        consumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        DefaultKafkaConsumerFactory<Object, Object> consumerFactory =
                new DefaultKafkaConsumerFactory<>(consumerProperties);
        return new KafkaClientResources(producerFactory, kafkaTemplate, consumerFactory);
    }

    private record KafkaClientResources(
            DefaultKafkaProducerFactory<Object, Object> producerFactory,
            KafkaTemplate<Object, Object> kafkaTemplate,
            DefaultKafkaConsumerFactory<Object, Object> consumerFactory) implements AutoCloseable {
        @Override
        public void close() {
            kafkaTemplate.destroy();
            producerFactory.destroy();
        }
    }
}
