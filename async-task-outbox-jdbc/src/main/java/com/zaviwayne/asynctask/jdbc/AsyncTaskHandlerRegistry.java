package com.zaviwayne.asynctask.jdbc;

import com.zaviwayne.asynctask.core.AsyncTaskHandler;
import com.zaviwayne.asynctask.core.AsyncTaskMessageValidator;
import com.zaviwayne.asynctask.core.NoAsyncTaskHandlerException;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * 异步任务处理器注册表。
 *
 * @since 2026-08-26
 */
public final class AsyncTaskHandlerRegistry {
    /**
     * 按任务类型和版本索引的处理器。
     */
    private final Map<HandlerKey, AsyncTaskHandler<?>> handlers;

    /**
     * 创建异步任务处理器注册表。
     *
     * @param handlers 应用提供的处理器
     */
    public AsyncTaskHandlerRegistry(Collection<AsyncTaskHandler<?>> handlers) {
        Objects.requireNonNull(handlers, "处理器集合不能为空");
        Map<HandlerKey, AsyncTaskHandler<?>> indexedHandlers = new LinkedHashMap<>(handlers.size());
        for (AsyncTaskHandler<?> handler : handlers) {
            AsyncTaskHandler<?> requiredHandler = Objects.requireNonNull(handler, "异步任务处理器不能为空");
            HandlerKey key = handlerKey(requiredHandler);
            AsyncTaskHandler<?> previous = indexedHandlers.putIfAbsent(key, requiredHandler);
            if (previous != null) {
                throw new IllegalStateException("异步任务处理器重复注册: " + key);
            }
        }
        this.handlers = Map.copyOf(indexedHandlers);
    }

    /**
     * 查找指定任务类型和版本的处理器。
     *
     * @param taskType      任务类型
     * @param schemaVersion 消息结构版本
     * @return 匹配的处理器
     * @throws NoAsyncTaskHandlerException 未注册匹配处理器时抛出
     */
    public AsyncTaskHandler<?> getRequired(String taskType, int schemaVersion) {
        HandlerKey key = new HandlerKey(
                AsyncTaskMessageValidator.validateTaskType(taskType),
                requireSchemaVersion(schemaVersion));
        AsyncTaskHandler<?> handler = handlers.get(key);
        if (handler == null) {
            throw new NoAsyncTaskHandlerException(
                    "未注册异步任务处理器: taskType=" + taskType + ", schemaVersion=" + schemaVersion);
        }
        return handler;
    }

    private static HandlerKey handlerKey(AsyncTaskHandler<?> handler) {
        String taskType = AsyncTaskMessageValidator.validateTaskType(handler.taskType());
        int schemaVersion = requireSchemaVersion(handler.schemaVersion());
        Objects.requireNonNull(
                handler.payloadType(), "异步任务处理器载荷类型不能为空: taskType=" + taskType);
        return new HandlerKey(taskType, schemaVersion);
    }

    private static int requireSchemaVersion(int schemaVersion) {
        if (schemaVersion <= 0) {
            throw new IllegalArgumentException("异步任务处理器消息结构版本必须大于 0");
        }
        return schemaVersion;
    }

    private record HandlerKey(String taskType, int schemaVersion) {
        @Override
        public String toString() {
            return taskType + ":" + schemaVersion;
        }
    }
}
