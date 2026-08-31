package com.zaviwayne.asynctask.autoconfigure.properties;

import org.springframework.boot.context.properties.bind.ConstructorBinding;

import java.util.regex.Pattern;

/**
 * 单个 Kafka 业务主题配置。
 *
 * @param topic           业务主题
 * @param deadLetterTopic 死信主题，留空时使用业务主题加默认后缀
 * @param parkingTopic    停放主题，留空时使用死信主题加默认后缀
 * @param consumerGroup   消费组，留空时使用全局默认值
 * @param concurrency     消费并发数，0 表示使用全局默认值
 * @param partitions      分区数，0 表示使用全局默认值
 * @param replicas        副本数，0 表示使用全局默认值
 * @since 2026-08-27
 */
public record AsyncTaskTopicBinding(String topic,
                                    String deadLetterTopic,
                                    String parkingTopic,
                                    String consumerGroup,
                                    int concurrency,
                                    int partitions,
                                    int replicas) {
    /**
     * Kafka 主题名称最大长度。
     */
    private static final int MAX_TOPIC_NAME_LENGTH = 249;

    /**
     * Kafka 主题名称允许的字符格式。
     */
    private static final Pattern TOPIC_NAME_PATTERN = Pattern.compile("[A-Za-z0-9._-]+");

    /**
     * Kafka 禁止使用的单点主题名称。
     */
    private static final String SINGLE_DOT_TOPIC_NAME = ".";

    /**
     * Kafka 禁止使用的双点主题名称。
     */
    private static final String DOUBLE_DOT_TOPIC_NAME = "..";

    /**
     * 规范化并校验单主题配置。
     */
    @ConstructorBinding
    public AsyncTaskTopicBinding {
        topic = normalizeTopic(topic, "Kafka 业务主题");
        deadLetterTopic = normalizeOptionalTopic(deadLetterTopic, "Kafka 死信主题");
        parkingTopic = normalizeOptionalTopic(parkingTopic, "Kafka 停放主题");
        consumerGroup = normalizeOptional(consumerGroup);
        if (concurrency < 0) {
            throw new IllegalArgumentException("Kafka 主题消费并发数不能小于 0");
        }
        if (partitions < 0) {
            throw new IllegalArgumentException("Kafka 主题分区数不能小于 0");
        }
        if (replicas < 0) {
            throw new IllegalArgumentException("Kafka 主题副本数不能小于 0");
        }
    }

    /**
     * 使用默认停放主题创建单主题配置。
     *
     * @param topic           业务主题
     * @param deadLetterTopic 死信主题
     * @param consumerGroup   消费组
     * @param concurrency     消费并发数
     * @param partitions      分区数
     * @param replicas        副本数
     */
    public AsyncTaskTopicBinding(String topic,
                                 String deadLetterTopic,
                                 String consumerGroup,
                                 int concurrency,
                                 int partitions,
                                 int replicas) {
        this(topic, deadLetterTopic, null, consumerGroup, concurrency, partitions, replicas);
    }

    private static String normalizeTopic(String value, String fieldName) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(fieldName + "不能为空");
        }
        String normalizedValue = value.trim();
        if (normalizedValue.length() > MAX_TOPIC_NAME_LENGTH) {
            throw new IllegalArgumentException(fieldName + "长度不能超过 " + MAX_TOPIC_NAME_LENGTH);
        }
        boolean reservedName = SINGLE_DOT_TOPIC_NAME.equals(normalizedValue)
                || DOUBLE_DOT_TOPIC_NAME.equals(normalizedValue);
        if (reservedName) {
            throw new IllegalArgumentException(fieldName + "不能为 . 或 ..");
        }
        if (!TOPIC_NAME_PATTERN.matcher(normalizedValue).matches()) {
            throw new IllegalArgumentException(fieldName + "只能包含英文字母、数字、点、下划线和连字符");
        }
        return normalizedValue;
    }

    private static String normalizeOptionalTopic(String value, String fieldName) {
        return value == null || value.isBlank() ? "" : normalizeTopic(value, fieldName);
    }

    private static String normalizeOptional(String value) {
        return value == null ? "" : value.trim();
    }
}
