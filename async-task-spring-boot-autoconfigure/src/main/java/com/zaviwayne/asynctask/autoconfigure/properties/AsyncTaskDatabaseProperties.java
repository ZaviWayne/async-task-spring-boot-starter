package com.zaviwayne.asynctask.autoconfigure.properties;

import org.springframework.boot.context.properties.bind.DefaultValue;

import java.util.Objects;

/**
 * 异步任务数据库配置。
 *
 * @param platform         数据库平台
 * @param initializeSchema 是否执行内置无破坏性建表脚本
 * @since 2026-08-26
 */
public record AsyncTaskDatabaseProperties(
        @DefaultValue("AUTO") DatabasePlatform platform,
        @DefaultValue("false") boolean initializeSchema) {
    /**
     * 校验数据库配置。
     */
    public AsyncTaskDatabaseProperties {
        Objects.requireNonNull(platform, "数据库平台不能为空");
    }
}
