package com.zaviwayne.asynctask.core;

import java.nio.charset.StandardCharsets;
import java.util.Objects;

/**
 * 异步任务内容大小限制。
 *
 * @param maxEnvelopeBytes 任务信封 JSON 最大 UTF-8 字节数
 * @param maxProgressBytes 任务进度 JSON 最大 UTF-8 字节数
 * @since 2026-08-31
 */
public record AsyncTaskContentLimits(int maxEnvelopeBytes, int maxProgressBytes) {
    /**
     * 默认任务信封 JSON 最大 UTF-8 字节数。
     */
    public static final int DEFAULT_MAX_ENVELOPE_BYTES = 1_000_000;

    /**
     * 默认任务进度 JSON 最大 UTF-8 字节数。
     */
    public static final int DEFAULT_MAX_PROGRESS_BYTES = 1_000_000;

    /**
     * 任务信封 JSON 字段名称。
     */
    private static final String ENVELOPE_JSON_FIELD_NAME = "任务信封 JSON";

    /**
     * 任务进度 JSON 字段名称。
     */
    private static final String PROGRESS_JSON_FIELD_NAME = "任务进度 JSON";

    /**
     * 校验异步任务内容大小限制。
     */
    public AsyncTaskContentLimits {
        if (maxEnvelopeBytes <= 0) {
            throw new IllegalArgumentException("任务信封 JSON 最大字节数必须大于 0");
        }
        if (maxProgressBytes <= 0) {
            throw new IllegalArgumentException("任务进度 JSON 最大字节数必须大于 0");
        }
    }

    /**
     * 创建默认内容大小限制。
     *
     * @return 默认内容大小限制
     */
    public static AsyncTaskContentLimits defaults() {
        return new AsyncTaskContentLimits(DEFAULT_MAX_ENVELOPE_BYTES, DEFAULT_MAX_PROGRESS_BYTES);
    }

    /**
     * 校验任务信封 JSON 的 UTF-8 字节数。
     *
     * @param envelopeJson 任务信封 JSON
     * @throws NullPointerException     任务信封 JSON 为空时抛出
     * @throws IllegalArgumentException 任务信封 JSON 超出限制时抛出
     */
    public void validateEnvelopeJson(String envelopeJson) {
        validateJsonSize(envelopeJson, maxEnvelopeBytes, ENVELOPE_JSON_FIELD_NAME);
    }

    /**
     * 校验任务进度 JSON 的 UTF-8 字节数。
     *
     * @param progressJson 任务进度 JSON
     * @throws NullPointerException     任务进度 JSON 为空时抛出
     * @throws IllegalArgumentException 任务进度 JSON 超出限制时抛出
     */
    public void validateProgressJson(String progressJson) {
        validateJsonSize(progressJson, maxProgressBytes, PROGRESS_JSON_FIELD_NAME);
    }

    private static void validateJsonSize(String json, int maximumBytes, String fieldName) {
        Objects.requireNonNull(json, fieldName + " 不能为空");
        int actualBytes = json.getBytes(StandardCharsets.UTF_8).length;
        if (actualBytes > maximumBytes) {
            throw new IllegalArgumentException(
                    fieldName + " 不能超过 " + maximumBytes + " 个 UTF-8 字节，实际为 " + actualBytes + " 字节");
        }
    }
}
