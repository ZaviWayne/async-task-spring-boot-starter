package com.zaviwayne.asynctask.autoconfigure.properties;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class AsyncTaskPropertiesTest {
    @Test
    void shouldResolvePerTopicOverridesAndLegacyTopics() {
        AsyncTaskKafkaProperties kafka = new AsyncTaskKafkaProperties(
            true,
            true,
            false,
            List.of("legacy-events", "custom-events"),
            List.of(new AsyncTaskTopicBinding(
                "custom-events", "custom-events-dead", "custom-workers", 4, 6, 2)),
            "default-workers",
            2,
            3,
            1,
            Duration.ofSeconds(10),
            Duration.ofSeconds(2),
            3,
            Duration.ofSeconds(5),
            ".DLT");

        assertThat(kafka.bindings()).hasSize(2);
        assertThat(kafka.bindings().getFirst())
            .extracting(
                AsyncTaskTopicBinding::topic,
                AsyncTaskTopicBinding::deadLetterTopic,
                AsyncTaskTopicBinding::parkingTopic,
                AsyncTaskTopicBinding::consumerGroup,
                AsyncTaskTopicBinding::concurrency,
                AsyncTaskTopicBinding::partitions,
                AsyncTaskTopicBinding::replicas)
            .containsExactly(
                "legacy-events", "legacy-events.DLT", "legacy-events.DLT.PARKING",
                "default-workers", 2, 3, 1);
        assertThat(kafka.bindings().getLast())
            .extracting(
                AsyncTaskTopicBinding::topic,
                AsyncTaskTopicBinding::deadLetterTopic,
                AsyncTaskTopicBinding::parkingTopic,
                AsyncTaskTopicBinding::consumerGroup,
                AsyncTaskTopicBinding::concurrency,
                AsyncTaskTopicBinding::partitions,
                AsyncTaskTopicBinding::replicas)
            .containsExactly(
                "custom-events", "custom-events-dead", "custom-events-dead.PARKING",
                "custom-workers", 4, 6, 2);
    }

    @Test
    void shouldRejectDispatchLeaseThatCannotCoverWorstCaseBatchSendTime() {
        AsyncTaskOutboxProperties outbox = new AsyncTaskOutboxProperties(
            true,
            Duration.ofSeconds(1),
            20,
            Duration.ofSeconds(30),
            Duration.ofMinutes(5),
            Duration.ofSeconds(30),
            2,
            8,
            Duration.ofSeconds(2),
            Duration.ofHours(1));
        AsyncTaskKafkaProperties kafka = new AsyncTaskKafkaProperties(
            true,
            false,
            false,
            List.of("task-events"),
            List.of(),
            "workers",
            1,
            3,
            1,
            Duration.ofSeconds(10),
            Duration.ofSeconds(2),
            3,
            Duration.ofSeconds(5),
            ".DLT");

        assertThatThrownBy(() -> new AsyncTaskProperties(
            true,
            new AsyncTaskDatabaseProperties(DatabasePlatform.AUTO, false),
            outbox,
            new AsyncTaskRetentionProperties(
                false, Duration.ofDays(30), Duration.ofHours(1), 500),
            kafka,
            new AsyncTaskObservabilityProperties(Duration.ofSeconds(30))))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("Outbox 投递租约时长必须大于批量大小与 Kafka 发送超时时间的乘积");
    }

    @Test
    void shouldRejectInvalidExplicitDeadLetterTopic() {
        assertThatThrownBy(() -> new AsyncTaskTopicBinding(
            "task-events", "invalid topic", "workers", 1, 3, 1))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("Kafka 死信主题只能包含英文字母、数字、点、下划线和连字符");
    }

    @Test
    void shouldRejectDerivedDeadLetterTopicThatExceedsKafkaLimit() {
        String businessTopic = "a".repeat(248);

        assertThatThrownBy(() -> kafkaProperties(businessTopic))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("Kafka 死信主题长度不能超过 249");
    }

    @Test
    void shouldRejectZeroKafkaRetryInterval() {
        assertThatThrownBy(() -> new AsyncTaskKafkaProperties(
            true,
            false,
            false,
            List.of("task-events"),
            List.of(),
            "workers",
            1,
            3,
            1,
            Duration.ofSeconds(10),
            Duration.ZERO,
            3,
            Duration.ofSeconds(5),
            ".DLT"))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("Kafka 消费重试间隔必须大于 0");
    }

    @Test
    void shouldRejectSubMillisecondDurationsConvertedToMilliseconds() {
        Duration subMillisecond = Duration.ofNanos(1);

        assertThatThrownBy(() -> new AsyncTaskOutboxProperties(
            true,
            subMillisecond,
            20,
            Duration.ofMinutes(5),
            Duration.ofMinutes(5),
            Duration.ofSeconds(30),
            2,
            8,
            Duration.ofSeconds(2),
            Duration.ofHours(1)))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("Outbox 轮询间隔必须至少为 1 毫秒");
        assertThatThrownBy(() -> new AsyncTaskRetentionProperties(
            false, Duration.ofDays(30), subMillisecond, 500))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("终态任务清理间隔必须至少为 1 毫秒");
        assertThatThrownBy(() -> kafkaProperties(
            subMillisecond, Duration.ofSeconds(2), Duration.ofSeconds(5)))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("Kafka 发送确认超时时间必须至少为 1 毫秒");
        assertThatThrownBy(() -> kafkaProperties(
            Duration.ofSeconds(10), subMillisecond, Duration.ofSeconds(5)))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("Kafka 消费重试间隔必须至少为 1 毫秒");
        assertThatThrownBy(() -> kafkaProperties(
            Duration.ofSeconds(10), Duration.ofSeconds(2), subMillisecond))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("Kafka 租约冲突重试间隔必须至少为 1 毫秒");
    }

    @Test
    void shouldRejectZeroExecutionHeartbeatThreads() {
        assertThatThrownBy(() -> new AsyncTaskOutboxProperties(
            true,
            Duration.ofSeconds(1),
            20,
            Duration.ofMinutes(5),
            Duration.ofMinutes(5),
            Duration.ofSeconds(30),
            0,
            8,
            Duration.ofSeconds(2),
            Duration.ofHours(1)))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("Outbox 执行心跳线程数必须大于 0");
    }

    @Test
    void shouldRejectNonPositiveContentLimits() {
        assertThatThrownBy(() -> new AsyncTaskOutboxProperties(
            true,
            Duration.ofSeconds(1),
            20,
            Duration.ofMinutes(5),
            Duration.ofMinutes(5),
            Duration.ofSeconds(30),
            2,
            8,
            Duration.ofSeconds(2),
            Duration.ofHours(1),
            0,
            1))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("任务信封 JSON 最大字节数必须大于 0");
    }

    @Test
    void shouldRejectZeroStatisticsCacheDuration() {
        assertThatThrownBy(() -> new AsyncTaskObservabilityProperties(Duration.ZERO))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("运行统计缓存时长必须大于 0");
    }

    private static AsyncTaskKafkaProperties kafkaProperties(String topic) {
        return kafkaProperties(
            topic, Duration.ofSeconds(10), Duration.ofSeconds(2), Duration.ofSeconds(5));
    }

    private static AsyncTaskKafkaProperties kafkaProperties(Duration sendTimeout,
                                                             Duration retryInterval,
                                                             Duration leaseRetryInterval) {
        return kafkaProperties("task-events", sendTimeout, retryInterval, leaseRetryInterval);
    }

    private static AsyncTaskKafkaProperties kafkaProperties(String topic,
                                                             Duration sendTimeout,
                                                             Duration retryInterval,
                                                             Duration leaseRetryInterval) {
        return new AsyncTaskKafkaProperties(
            true,
            false,
            false,
            List.of(topic),
            List.of(),
            "workers",
            1,
            3,
            1,
            sendTimeout,
            retryInterval,
            3,
            leaseRetryInterval,
            ".DLT");
    }
}
