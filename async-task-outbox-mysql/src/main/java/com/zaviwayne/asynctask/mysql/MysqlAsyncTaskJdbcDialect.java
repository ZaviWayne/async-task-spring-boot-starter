package com.zaviwayne.asynctask.mysql;

import com.zaviwayne.asynctask.jdbc.AsyncTaskJdbcDialect;

/**
 * MySQL 异步任务 JDBC 数据库方言。
 *
 * @since 2026-08-26
 */
public final class MysqlAsyncTaskJdbcDialect implements AsyncTaskJdbcDialect {
    /**
     * MySQL 数据库产品名称。
     */
    private static final String DATABASE_PRODUCT_NAME = "MySQL";

    /**
     * MySQL 建表资源路径。
     */
    private static final String SCHEMA_RESOURCE = "db/async-task-mysql-schema.sql";

    /**
     * Outbox 幂等插入 SQL。
     */
    private static final String INSERT_OUTBOX_SQL = """
            INSERT INTO async_task_outbox (
                task_id, destination, task_type, schema_version,
                idempotency_key, reference_type, reference_id,
                payload_hash, envelope_json, generation,
                status, dispatch_attempt, created_at, updated_at
            ) VALUES (
                :taskId, :destination, :taskType, :schemaVersion,
                :idempotencyKey, :referenceType, :referenceId,
                :payloadHash, :envelopeJson, :generation,
                :status, 0, :now, :now
            )
            """;

    /**
     * 获取支持的数据库产品名称。
     *
     * @return 数据库产品名称
     */
    @Override
    public String databaseProductName() {
        return DATABASE_PRODUCT_NAME;
    }

    /**
     * 获取 outbox 幂等插入 SQL。
     *
     * @return 使用命名参数的插入 SQL
     */
    @Override
    public String insertOutboxSql() {
        return INSERT_OUTBOX_SQL;
    }

    /**
     * 保持 MySQL CHAR(36) 使用字符串参数。
     *
     * @param value UUID 文本
     * @return UUID 字符串参数
     */
    @Override
    public Object uuidParameter(String value) {
        return value;
    }

    /**
     * 获取无破坏性的建表脚本路径。
     *
     * @return classpath 资源路径
     */
    @Override
    public String schemaResource() {
        return SCHEMA_RESOURCE;
    }
}
