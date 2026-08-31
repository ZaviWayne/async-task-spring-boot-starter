package com.zaviwayne.asynctask.postgresql;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Locale;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * PostgreSQL JDBC 方言测试。
 *
 * @since 2026-08-26
 */
class PostgresqlAsyncTaskJdbcDialectTest {
    /**
     * 数据库方言。
     */
    private final PostgresqlAsyncTaskJdbcDialect dialect = new PostgresqlAsyncTaskJdbcDialect();

    @Test
    void shouldExposePostgreSqlProductAndSchemaResource() {
        assertThat(dialect.databaseProductName()).isEqualTo("PostgreSQL");
        assertThat(dialect.schemaResource()).isEqualTo("db/async-task-postgresql-schema.sql");
        String uuid = "00000000-0000-0000-0000-000000000001";
        assertThat(dialect.uuidParameter(uuid)).isEqualTo(UUID.fromString(uuid));
    }

    @Test
    void shouldUseIdempotentPostgreSqlInserts() {
        assertThat(dialect.insertOutboxSql())
                .contains("INSERT INTO async_task_outbox", ":taskId", ":envelopeJson", "ON CONFLICT DO NOTHING")
                .contains("dispatch_attempt", "generation");
    }

    @Test
    void shouldProvideNonDestructivePostgreSqlSchema() throws IOException {
        String schema = readSchema();
        String normalizedSchema = schema.toUpperCase(Locale.ROOT);

        assertThat(normalizedSchema).doesNotContain("DROP ");
        assertThat(normalizedSchema)
                .contains("CREATE TABLE IF NOT EXISTS ASYNC_TASK_OUTBOX")
                .containsOnlyOnce("CREATE TABLE IF NOT EXISTS")
                .contains("TIMESTAMPTZ")
                .contains("EXECUTION_ATTEMPT")
                .contains("GENERATION")
                .contains("PROGRESS_JSON")
                .contains("COMPLETED_AT")
                .contains("UNIQUE (IDEMPOTENCY_KEY)")
                .contains("CHECK")
                .contains("IDX_ASYNC_TASK_OUTBOX_PENDING_CLAIM")
                .contains("IDX_ASYNC_TASK_OUTBOX_RETRY_CLAIM")
                .contains("IDX_ASYNC_TASK_OUTBOX_LEASE_CLAIM");
        assertThat(schema).containsOnlyOnce("8-投递结果未知等待重试");
    }

    private String readSchema() throws IOException {
        InputStream inputStream = getClass().getClassLoader().getResourceAsStream(dialect.schemaResource());
        assertThat(inputStream).as("PostgreSQL schema resource").isNotNull();
        try (InputStream schemaStream = inputStream) {
            return new String(schemaStream.readAllBytes(), StandardCharsets.UTF_8);
        }
    }
}
