package com.zaviwayne.asynctask.core;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Collections;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * 异步任务消息字段校验器。
 *
 * @since 2026-08-31
 */
public final class AsyncTaskMessageValidator {
    /**
     * Kafka 主题允许的最大长度。
     */
    private static final int MAX_DESTINATION_LENGTH = 249;

    /**
     * 任务类型最大长度。
     */
    private static final int MAX_TASK_TYPE_LENGTH = 200;

    /**
     * 幂等键最大长度。
     */
    private static final int MAX_IDEMPOTENCY_KEY_LENGTH = 255;

    /**
     * 业务关联类型最大长度。
     */
    private static final int MAX_REFERENCE_TYPE_LENGTH = 100;

    /**
     * 业务关联标识最大长度。
     */
    private static final int MAX_REFERENCE_ID_LENGTH = 255;

    /**
     * 请求头最大数量。
     */
    private static final int MAX_HEADER_COUNT = 64;

    /**
     * 请求头名称最大长度。
     */
    private static final int MAX_HEADER_NAME_LENGTH = 100;

    /**
     * 请求头值最大长度。
     */
    private static final int MAX_HEADER_VALUE_LENGTH = 1000;

    /**
     * SHA-256 小写十六进制摘要格式。
     */
    private static final Pattern PAYLOAD_HASH_PATTERN = Pattern.compile("[0-9a-f]{64}");

    /**
     * 载荷摘要算法。
     */
    private static final String PAYLOAD_HASH_ALGORITHM = "SHA-256";

    private AsyncTaskMessageValidator() {
        throw new IllegalStateException("异步任务消息字段校验器不能实例化");
    }

    /**
     * 校验并规范化目标通道。
     *
     * @param destination 目标通道
     * @return 规范化后的目标通道
     */
    public static String validateDestination(String destination) {
        return requireText(destination, "目标通道", MAX_DESTINATION_LENGTH);
    }

    /**
     * 校验并规范化任务类型。
     *
     * @param taskType 任务类型
     * @return 规范化后的任务类型
     */
    public static String validateTaskType(String taskType) {
        return requireText(taskType, "任务类型", MAX_TASK_TYPE_LENGTH);
    }

    /**
     * 校验并规范化 JSON 载荷。
     *
     * @param payloadJson JSON 载荷
     * @return 原始 JSON 载荷
     */
    public static String validatePayloadJson(String payloadJson) {
        if (payloadJson == null || payloadJson.isBlank()) {
            throw new IllegalArgumentException("JSON 载荷不能为空");
        }
        return payloadJson;
    }

    /**
     * 校验 SHA-256 载荷摘要。
     *
     * @param payloadJson JSON 载荷
     * @param payloadHash 载荷摘要
     * @return 合法的载荷摘要
     * @throws IllegalArgumentException 摘要格式错误或与 JSON 载荷内容不一致时抛出
     */
    public static String validatePayloadHash(String payloadJson, String payloadHash) {
        if (payloadHash == null || !PAYLOAD_HASH_PATTERN.matcher(payloadHash).matches()) {
            throw new IllegalArgumentException("载荷摘要必须是 64 位小写 SHA-256 十六进制字符串");
        }
        String calculatedPayloadHash = calculatePayloadHash(payloadJson);
        if (!calculatedPayloadHash.equals(payloadHash)) {
            throw new IllegalArgumentException("载荷摘要与 JSON 载荷内容不一致");
        }
        return payloadHash;
    }

    /**
     * 计算 JSON 载荷的 SHA-256 摘要。
     *
     * @param payloadJson JSON 载荷
     * @return 64 位小写十六进制摘要
     * @throws IllegalArgumentException JSON 载荷为空时抛出
     * @throws IllegalStateException    当前 JVM 不支持 SHA-256 时抛出
     */
    public static String calculatePayloadHash(String payloadJson) {
        String validatedPayloadJson = validatePayloadJson(payloadJson);
        try {
            MessageDigest messageDigest = MessageDigest.getInstance(PAYLOAD_HASH_ALGORITHM);
            byte[] digest = messageDigest.digest(validatedPayloadJson.getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(digest);
        } catch (NoSuchAlgorithmException exception) {
            throw new IllegalStateException("当前 JVM 不支持 SHA-256 摘要算法", exception);
        }
    }

    /**
     * 校验并规范化幂等键。
     *
     * @param idempotencyKey 幂等键
     * @return 规范化后的幂等键
     */
    public static String validateIdempotencyKey(String idempotencyKey) {
        return requireText(idempotencyKey, "幂等键", MAX_IDEMPOTENCY_KEY_LENGTH);
    }

    /**
     * 规范化可选业务关联类型。
     *
     * @param referenceType 业务关联类型
     * @return 规范化后的业务关联类型，可为空
     */
    public static String normalizeReferenceType(String referenceType) {
        return normalizeOptional(referenceType, "业务关联类型", MAX_REFERENCE_TYPE_LENGTH);
    }

    /**
     * 规范化可选业务关联标识。
     *
     * @param referenceId 业务关联标识
     * @return 规范化后的业务关联标识，可为空
     */
    public static String normalizeReferenceId(String referenceId) {
        return normalizeOptional(referenceId, "业务关联标识", MAX_REFERENCE_ID_LENGTH);
    }

    /**
     * 校验业务关联类型和标识必须成对出现。
     *
     * @param referenceType 业务关联类型
     * @param referenceId   业务关联标识
     */
    public static void validateReferencePair(String referenceType, String referenceId) {
        if ((referenceType == null) != (referenceId == null)) {
            throw new IllegalArgumentException("业务关联类型和业务关联标识必须同时填写或同时留空");
        }
    }

    /**
     * 校验、规范化并复制扩展请求头。
     *
     * @param headers 扩展请求头
     * @return 不可变的规范化请求头
     */
    public static Map<String, String> validateHeaders(Map<String, String> headers) {
        if (headers == null || headers.isEmpty()) {
            return Map.of();
        }
        if (headers.size() > MAX_HEADER_COUNT) {
            throw new IllegalArgumentException("扩展请求头数量不能超过 " + MAX_HEADER_COUNT);
        }
        Map<String, String> copiedHeaders = new LinkedHashMap<>(headers.size());
        headers.forEach((name, value) -> putHeader(copiedHeaders, name, value));
        return Collections.unmodifiableMap(copiedHeaders);
    }

    private static void putHeader(Map<String, String> headers, String name, String value) {
        String normalizedName = requireText(name, "请求头名称", MAX_HEADER_NAME_LENGTH);
        String normalizedValue = requireText(value, "请求头值", MAX_HEADER_VALUE_LENGTH);
        if (headers.putIfAbsent(normalizedName, normalizedValue) != null) {
            throw new IllegalArgumentException("规范化后的请求头名称不能重复: " + normalizedName);
        }
    }

    private static String requireText(String value, String fieldName, int maxLength) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(fieldName + "不能为空");
        }
        String normalizedValue = value.trim();
        if (normalizedValue.length() > maxLength) {
            throw new IllegalArgumentException(fieldName + "长度不能超过 " + maxLength);
        }
        return normalizedValue;
    }

    private static String normalizeOptional(String value, String fieldName, int maxLength) {
        if (value == null || value.isBlank()) {
            return null;
        }
        return requireText(value, fieldName, maxLength);
    }
}
