package com.zaviwayne.asynctask.autoconfigure.jdbc;

import com.zaviwayne.asynctask.autoconfigure.AsyncTaskAutoConfiguration;
import com.zaviwayne.asynctask.autoconfigure.properties.AsyncTaskProperties;
import com.zaviwayne.asynctask.autoconfigure.properties.DatabasePlatform;
import com.zaviwayne.asynctask.core.*;
import com.zaviwayne.asynctask.jdbc.*;
import com.zaviwayne.asynctask.mysql.MysqlAsyncTaskJdbcDialect;
import com.zaviwayne.asynctask.postgresql.PostgresqlAsyncTaskJdbcDialect;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.support.JdbcTransactionManager;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.time.Clock;
import java.util.List;

/**
 * 异步任务 JDBC 自动配置。
 *
 * @since 2026-08-27
 */
@AutoConfiguration(
        after = AsyncTaskAutoConfiguration.class,
        afterName = "org.springframework.boot.jdbc.autoconfigure.DataSourceAutoConfiguration")
@ConditionalOnClass({DataSource.class, JdbcTaskStore.class})
@ConditionalOnBean(DataSource.class)
@ConditionalOnProperty(prefix = "async-task", name = "enabled", havingValue = "true", matchIfMissing = true)
public class AsyncTaskJdbcAutoConfiguration {
    /**
     * PostgreSQL 产品名称。
     */
    private static final String POSTGRESQL_PRODUCT_NAME = "PostgreSQL";

    /**
     * MySQL 产品名称。
     */
    private static final String MYSQL_PRODUCT_NAME = "MySQL";

    /**
     * PostgreSQL 最低主版本。
     */
    private static final int POSTGRESQL_MINIMUM_MAJOR_VERSION = 13;

    /**
     * MySQL 最低主版本。
     */
    private static final int MYSQL_MINIMUM_MAJOR_VERSION = 8;

    /**
     * 创建符合 JDBC 元数据的数据库方言。
     *
     * @param dataSource 数据源
     * @param properties starter 配置
     * @return 数据库方言
     */
    @Bean
    @ConditionalOnMissingBean
    public AsyncTaskJdbcDialect asyncTaskJdbcDialect(DataSource dataSource, AsyncTaskProperties properties) {
        DatabasePlatform configuredPlatform = properties.database().platform();
        DatabasePlatform detectedPlatform = detectPlatform(dataSource);
        if (configuredPlatform != DatabasePlatform.AUTO && configuredPlatform != detectedPlatform) {
            throw new IllegalStateException("配置的数据库平台与 JDBC 元数据不一致: configured="
                    + configuredPlatform + ", detected=" + detectedPlatform);
        }
        DatabasePlatform platform = configuredPlatform == DatabasePlatform.AUTO
                ? detectedPlatform
                : configuredPlatform;
        return switch (platform) {
            case POSTGRESQL -> new PostgresqlAsyncTaskJdbcDialect();
            case MYSQL -> new MysqlAsyncTaskJdbcDialect();
            case AUTO -> throw new IllegalStateException("无法自动识别异步任务数据库平台");
        };
    }

    /**
     * 创建 JDBC 异步任务状态存储。
     *
     * @param dataSource          数据源
     * @param dialect             数据库方言
     * @param serializer          任务载荷序列化器
     * @param observer            任务生命周期观察器
     * @param contentLimits       任务内容大小限制
     * @param transactionManagers 应用 JDBC 事务管理器提供器
     * @return JDBC 状态存储
     */
    @Bean
    @ConditionalOnMissingBean
    public JdbcTaskStore asyncTaskJdbcStore(DataSource dataSource,
                                            AsyncTaskJdbcDialect dialect,
                                            TaskPayloadSerializer serializer,
                                            AsyncTaskObserver observer,
                                            AsyncTaskContentLimits contentLimits,
                                            ObjectProvider<DataSourceTransactionManager> transactionManagers) {
        List<DataSourceTransactionManager> matchingTransactionManagers = transactionManagers.stream()
                .filter(transactionManager -> transactionManager.getDataSource() == dataSource)
                .toList();
        if (matchingTransactionManagers.size() > 1) {
            throw new IllegalStateException("存在多个绑定到异步任务数据源的 JDBC 事务管理器");
        }
        PlatformTransactionManager transactionManager = matchingTransactionManagers.isEmpty()
                ? new JdbcTransactionManager(dataSource)
                : matchingTransactionManagers.getFirst();
        return new JdbcTaskStore(
                new NamedParameterJdbcTemplate(dataSource),
                transactionManager,
                dialect,
                serializer,
                observer,
                contentLimits);
    }

    /**
     * 创建异步任务查询管理门面。
     *
     * @param taskStore JDBC 状态存储
     * @param clock     UTC 时钟
     * @return 异步任务查询管理门面
     */
    @Bean
    @ConditionalOnMissingBean(AsyncTaskAdmin.class)
    public AsyncTaskAdmin asyncTaskAdmin(JdbcTaskStore taskStore, Clock clock) {
        return new JdbcAsyncTaskAdmin(taskStore, clock);
    }

    /**
     * 创建任务发起门面。
     *
     * @param taskStore  JDBC 状态存储
     * @param serializer 任务载荷序列化器
     * @param clock      UTC 时钟
     * @return 异步任务发起器
     */
    @Bean
    @ConditionalOnMissingBean(AsyncTaskEnqueuer.class)
    public AsyncTaskEnqueuer asyncTaskEnqueuer(JdbcTaskStore taskStore,
                                               TaskPayloadSerializer serializer,
                                               Clock clock) {
        return new JdbcAsyncTaskEnqueuer(taskStore, serializer, clock);
    }

    /**
     * 创建异步任务处理器。
     *
     * @param taskStore       JDBC 状态存储
     * @param handlerRegistry 业务处理器注册表
     * @param serializer      任务载荷序列化器
     * @param clock           UTC 时钟
     * @param properties      starter 配置
     * @param observer        任务生命周期观察器
     * @return 异步任务处理器
     */
    @Bean(destroyMethod = "close")
    @ConditionalOnMissingBean
    public AsyncTaskProcessor asyncTaskProcessor(JdbcTaskStore taskStore,
                                                 AsyncTaskHandlerRegistry handlerRegistry,
                                                 TaskPayloadSerializer serializer,
                                                 Clock clock,
                                                 AsyncTaskProperties properties,
                                                 AsyncTaskObserver observer) {
        return new AsyncTaskProcessor(
                taskStore, handlerRegistry, serializer, clock,
                properties.outbox().executionLeaseDuration(),
                properties.outbox().executionHeartbeatInterval(),
                properties.outbox().executionHeartbeatThreads(),
                observer);
    }

    /**
     * 创建可选的数据库脚本初始化器。
     *
     * @param dataSource 数据源
     * @param dialect    数据库方言
     * @param properties starter 配置
     * @return 数据库脚本初始化器
     */
    @Bean
    @ConditionalOnMissingBean
    public AsyncTaskSchemaInitializer asyncTaskSchemaInitializer(DataSource dataSource,
                                                                 AsyncTaskJdbcDialect dialect,
                                                                 AsyncTaskProperties properties) {
        return new AsyncTaskSchemaInitializer(
                dataSource, dialect.schemaResource(), properties.database().initializeSchema());
    }

    private static DatabasePlatform detectPlatform(DataSource dataSource) {
        try (Connection connection = dataSource.getConnection()) {
            DatabaseMetaData metadata = connection.getMetaData();
            String productName = metadata.getDatabaseProductName();
            int majorVersion = metadata.getDatabaseMajorVersion();
            if (POSTGRESQL_PRODUCT_NAME.equalsIgnoreCase(productName)) {
                if (majorVersion < POSTGRESQL_MINIMUM_MAJOR_VERSION) {
                    throw new IllegalStateException("异步任务 starter 要求 PostgreSQL 13 或更高版本");
                }
                return DatabasePlatform.POSTGRESQL;
            }
            if (MYSQL_PRODUCT_NAME.equalsIgnoreCase(productName)) {
                if (majorVersion < MYSQL_MINIMUM_MAJOR_VERSION) {
                    throw new IllegalStateException("异步任务 starter 要求 MySQL 8.0 或更高版本");
                }
                return DatabasePlatform.MYSQL;
            }
            throw new IllegalStateException("异步任务 starter 不支持当前数据库: " + productName);
        } catch (SQLException exception) {
            throw new IllegalStateException("读取数据库产品信息失败", exception);
        }
    }
}
