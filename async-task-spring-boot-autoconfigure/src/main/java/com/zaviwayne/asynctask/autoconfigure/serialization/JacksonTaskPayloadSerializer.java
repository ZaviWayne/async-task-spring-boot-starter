package com.zaviwayne.asynctask.autoconfigure.serialization;

import com.zaviwayne.asynctask.core.AsyncTaskProcessingException;
import com.zaviwayne.asynctask.core.TaskPayloadSerializer;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.ObjectMapper;

import java.util.Objects;

/**
 * Jackson 3 任务载荷序列化器。
 *
 * @since 2026-08-26
 */
public final class JacksonTaskPayloadSerializer implements TaskPayloadSerializer {
    /**
     * Jackson 对象映射器。
     */
    private final ObjectMapper objectMapper;

    /**
     * 创建 Jackson 3 任务载荷序列化器。
     *
     * @param objectMapper Jackson 对象映射器
     */
    public JacksonTaskPayloadSerializer(ObjectMapper objectMapper) {
        this.objectMapper = Objects.requireNonNull(objectMapper, "Jackson 对象映射器不能为空");
    }

    /**
     * 将任务载荷序列化为 JSON。
     *
     * @param payload 载荷对象
     * @return JSON 字符串
     * @throws AsyncTaskProcessingException JSON 序列化失败时抛出
     */
    @Override
    public String serialize(Object payload) {
        try {
            return objectMapper.writeValueAsString(payload);
        } catch (JacksonException exception) {
            throw new AsyncTaskProcessingException("异步任务载荷序列化失败", exception);
        }
    }

    /**
     * 将 JSON 反序列化为目标类型。
     *
     * @param payloadJson JSON 字符串
     * @param payloadType 目标类型
     * @param <T>         目标类型
     * @return 反序列化结果
     * @throws AsyncTaskProcessingException JSON 反序列化失败时抛出
     */
    @Override
    public <T> T deserialize(String payloadJson, Class<T> payloadType) {
        try {
            return objectMapper.readValue(payloadJson, payloadType);
        } catch (JacksonException exception) {
            throw new AsyncTaskProcessingException("异步任务载荷反序列化失败", exception);
        }
    }
}
