package com.zaviwayne.asynctask.core;

/**
 * 任务载荷序列化器。
 *
 * @since 2026-08-26
 */
public interface TaskPayloadSerializer {
    /**
     * 将对象序列化为 JSON。
     *
     * @param payload 载荷对象
     * @return JSON 字符串
     */
    String serialize(Object payload);

    /**
     * 将 JSON 反序列化为目标类型。
     *
     * @param payloadJson JSON 字符串
     * @param payloadType 目标类型
     * @return 反序列化结果
     */
    <T> T deserialize(String payloadJson, Class<T> payloadType);
}
