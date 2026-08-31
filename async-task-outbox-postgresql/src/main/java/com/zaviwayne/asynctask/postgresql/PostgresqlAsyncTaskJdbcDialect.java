package com.zaviwayne.asynctask.postgresql;

import com.zaviwayne.asynctask.jdbc.AsyncTaskJdbcDialect;

import java.util.UUID;

/**
 * PostgreSQL 异步任务 JDBC 数据库方言。
 *
 * @since 2026-08-26
 */
public final class PostgresqlAsyncTaskJdbcDialect implements AsyncTaskJdbcDialect {
    /**
     * PostgreSQL 数据库产品名称。
     */
    private static final String DATABASE_PRODUCT_NAME = "PostgreSQL";

    /**
     * PostgreSQL 建表资源路径。
     */
    private static final String SCHEMA_RESOURCE = "db/async-task-postgresql-schema.sql";

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
            ON CONFLICT DO NOTHING
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
     * 将 UUID 文本转换为 PostgreSQL 原生 UUID 参数。
     *
     * @param value UUID 文本
     * @return PostgreSQL UUID 参数
     */
    @Override
    public Object uuidParameter(String value) {
        return UUID.fromString(value);
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
