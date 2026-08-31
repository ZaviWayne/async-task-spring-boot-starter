package com.zaviwayne.asynctask.autoconfigure.jdbc;

import org.springframework.beans.factory.InitializingBean;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.datasource.init.DatabasePopulatorUtils;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;

import javax.sql.DataSource;
import java.util.Objects;

/**
 * 异步任务数据库脚本初始化器。
 *
 * @since 2026-08-26
 */
public final class AsyncTaskSchemaInitializer implements InitializingBean {
    /**
     * 数据源。
     */
    private final DataSource dataSource;

    /**
     * 建表脚本路径。
     */
    private final String schemaResource;

    /**
     * 是否执行脚本。
     */
    private final boolean enabled;

    /**
     * 创建异步任务数据库脚本初始化器。
     *
     * @param dataSource     数据源
     * @param schemaResource 建表脚本路径
     * @param enabled        是否执行脚本
     */
    public AsyncTaskSchemaInitializer(DataSource dataSource, String schemaResource, boolean enabled) {
        this.dataSource = Objects.requireNonNull(dataSource, "数据源不能为空");
        this.schemaResource = Objects.requireNonNull(schemaResource, "建表脚本路径不能为空");
        this.enabled = enabled;
    }

    /**
     * 在启用时执行当前数据库方言的无破坏性建表脚本。
     */
    @Override
    public void afterPropertiesSet() {
        if (!enabled) {
            return;
        }
        ResourceDatabasePopulator populator = new ResourceDatabasePopulator(
                new ClassPathResource(schemaResource));
        DatabasePopulatorUtils.execute(populator, dataSource);
    }
}
