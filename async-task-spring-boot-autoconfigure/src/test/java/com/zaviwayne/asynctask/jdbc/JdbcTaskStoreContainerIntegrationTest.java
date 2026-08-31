package com.zaviwayne.asynctask.jdbc;

import com.zaviwayne.asynctask.core.*;
import com.zaviwayne.asynctask.mysql.MysqlAsyncTaskJdbcDialect;
import com.zaviwayne.asynctask.postgresql.PostgresqlAsyncTaskJdbcDialect;
import org.junit.jupiter.api.Test;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mysql.MySQLContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.*;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers(disabledWithoutDocker = true)
class JdbcTaskStoreContainerIntegrationTest {
    /**
     * 固定测试时间。
     */
    private static final Instant NOW = Instant.parse("2026-08-27T00:00:00Z");

    /**
     * PostgreSQL 测试容器。
     */
    @Container
    private static final PostgreSQLContainer POSTGRESQL = new PostgreSQLContainer("postgres:17.10")
            .withDatabaseName("async_task")
            .withUsername("async_task")
            .withPassword("async_task");

    /**
     * MySQL 测试容器。
     */
    @Container
    private static final MySQLContainer MYSQL = new MySQLContainer("mysql:8.4.10")
            .withDatabaseName("async_task")
            .withUsername("async_task")
            .withPassword("async_task");

    @Test
    void shouldRunStateMachineOnPostgresql() throws Exception {
        exerciseDatabase(POSTGRESQL, new PostgresqlAsyncTaskJdbcDialect(), false);
    }

    @Test
    void shouldRunStateMachineOnMysql() throws Exception {
        exerciseDatabase(MYSQL, new MysqlAsyncTaskJdbcDialect(), true);
    }

    private static void exerciseDatabase(JdbcDatabaseContainer<?> container,
                                         AsyncTaskJdbcDialect dialect,
                                         boolean mysql) throws Exception {
        DataSource dataSource = dataSource(container, mysql);
        new ResourceDatabasePopulator(new ClassPathResource(dialect.schemaResource())).execute(dataSource);
        DataSourceTransactionManager transactionManager = new DataSourceTransactionManager(dataSource);
        TransactionTemplate transactionTemplate = new TransactionTemplate(transactionManager);
        InMemoryEnvelopeSerializer serializer = new InMemoryEnvelopeSerializer();
        JdbcTaskStore taskStore = new JdbcTaskStore(
                new NamedParameterJdbcTemplate(dataSource), transactionManager, dialect, serializer);
        taskStore.checkHealth();
        AsyncTaskEnvelope firstEnvelope = envelope(UUID.randomUUID());
        AsyncTaskEnvelope secondEnvelope = envelope(UUID.randomUUID());

        List<UUID> persistedTaskIds = saveConcurrently(
                taskStore, transactionTemplate, firstEnvelope, secondEnvelope);
        assertThat(persistedTaskIds).hasSize(2).allMatch(persistedTaskIds.getFirst()::equals);
        UUID taskId = persistedTaskIds.getFirst();
        JdbcAsyncTaskAdmin taskAdmin = new JdbcAsyncTaskAdmin(
                taskStore, Clock.fixed(NOW.plusSeconds(20), ZoneOffset.UTC));

        assertThat(taskAdmin.findByReference("resume", "42", 20, 0))
                .extracting(AsyncTaskInfo::taskId)
                .containsExactly(taskId);
        assertLockedRowIsSkipped(dataSource, dialect, taskStore, taskId);

        TestOutboxClaim firstClaim = claimFirstOutbox(
                taskStore, 10, 3, NOW, Duration.ofSeconds(1));
        TestOutboxClaim recoveredClaim = claimFirstOutbox(
                taskStore, 10, 3, NOW.plusSeconds(2), Duration.ofSeconds(1));
        assertThat(taskStore.markOutboxDispatched(
                taskId, firstClaim.leaseToken(), NOW.plusSeconds(2))).isFalse();
        assertThat(taskStore.markOutboxDispatched(
                taskId, recoveredClaim.leaseToken(), NOW.plusSeconds(2))).isTrue();

        AsyncTaskEnvelope persistedEnvelope = serializer.envelope(taskId);
        String executionLeaseToken = taskStore.claimExecution(
                persistedEnvelope, NOW.plusSeconds(3), Duration.ofSeconds(2)).orElseThrow();
        assertThat(taskStore.updateExecutionProgress(
                taskId,
                executionLeaseToken,
                "{\"completed\":50}",
                NOW.plusSeconds(4),
                Duration.ofSeconds(2))).isTrue();
        assertThat(taskStore.markExecutionCompleted(
                taskId, executionLeaseToken, NOW.plusSeconds(5))).isTrue();

        AsyncTaskInfo completedTask = taskAdmin.findByTaskId(taskId).orElseThrow();
        assertThat(completedTask.status()).isEqualTo(AsyncTaskStatus.SUCCESS);
        assertThat(completedTask.progressJson()).isEqualTo("{\"completed\":50}");
        assertThat(taskAdmin.requeue(taskId)).isEqualTo(AsyncTaskRequeueResult.REQUEUED);
        assertThat(taskAdmin.findByTaskId(taskId).orElseThrow().status())
                .isEqualTo(AsyncTaskStatus.PENDING);

        completeRequeuedTask(taskStore, persistedEnvelope, taskId);
        assertThat(taskStore.deleteTerminalTasks(NOW.plusSeconds(30), 10)).isEqualTo(1);
        assertThat(taskAdmin.findByTaskId(taskId)).isEmpty();
    }

    private static List<UUID> saveConcurrently(JdbcTaskStore taskStore,
                                               TransactionTemplate transactionTemplate,
                                               AsyncTaskEnvelope firstEnvelope,
                                               AsyncTaskEnvelope secondEnvelope)
            throws InterruptedException, ExecutionException {
        CountDownLatch ready = new CountDownLatch(2);
        CountDownLatch start = new CountDownLatch(1);
        try (ExecutorService executor = Executors.newFixedThreadPool(2)) {
            Future<UUID> first = executor.submit(() -> save(
                    taskStore, transactionTemplate, firstEnvelope, ready, start));
            Future<UUID> second = executor.submit(() -> save(
                    taskStore, transactionTemplate, secondEnvelope, ready, start));
            ready.await();
            start.countDown();
            return List.of(first.get(), second.get());
        }
    }

    private static UUID save(JdbcTaskStore taskStore,
                             TransactionTemplate transactionTemplate,
                             AsyncTaskEnvelope envelope,
                             CountDownLatch ready,
                             CountDownLatch start) throws InterruptedException {
        ready.countDown();
        start.await();
        return transactionTemplate.execute(status -> taskStore.saveOutbox(envelope));
    }

    private static void assertLockedRowIsSkipped(DataSource dataSource,
                                                 AsyncTaskJdbcDialect dialect,
                                                 JdbcTaskStore taskStore,
                                                 UUID taskId) throws Exception {
        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(
                     "SELECT task_id FROM async_task_outbox WHERE task_id = ? FOR UPDATE")) {
            connection.setAutoCommit(false);
            statement.setObject(1, dialect.uuidParameter(taskId.toString()));
            statement.executeQuery();

            assertThat(taskStore.claimOutbox(
                    10, 3, NOW, Duration.ofSeconds(1), UUID.randomUUID().toString())).isEmpty();
            connection.rollback();
        }
    }

    private static void completeRequeuedTask(JdbcTaskStore taskStore,
                                             AsyncTaskEnvelope envelope,
                                             UUID taskId) {
        TestOutboxClaim dispatchClaim = claimFirstOutbox(
                taskStore, 10, 3, NOW.plusSeconds(21), Duration.ofSeconds(2));
        taskStore.markOutboxDispatched(taskId, dispatchClaim.leaseToken(), NOW.plusSeconds(22));
        String executionLeaseToken = taskStore.claimExecution(
                dispatchClaim.envelope(), NOW.plusSeconds(23), Duration.ofSeconds(2)).orElseThrow();
        taskStore.markExecutionCompleted(taskId, executionLeaseToken, NOW.plusSeconds(24));
    }

    private static TestOutboxClaim claimFirstOutbox(JdbcTaskStore taskStore,
                                                    int batchSize,
                                                    int maxAttempts,
                                                    Instant now,
                                                    Duration leaseDuration) {
        String leaseToken = UUID.randomUUID().toString();
        Map<AsyncTaskEnvelope, Integer> claimedEnvelopes = taskStore.claimOutbox(
                batchSize, maxAttempts, now, leaseDuration, leaseToken);
        Map.Entry<AsyncTaskEnvelope, Integer> claim = claimedEnvelopes.entrySet()
                .stream()
                .findFirst()
                .orElseThrow();
        return new TestOutboxClaim(leaseToken, claim.getValue(), claim.getKey());
    }

    private static DataSource dataSource(JdbcDatabaseContainer<?> container, boolean mysql) {
        DriverManagerDataSource dataSource = new DriverManagerDataSource();
        String jdbcUrl = container.getJdbcUrl();
        if (mysql) {
            jdbcUrl = jdbcUrl + (jdbcUrl.contains("?") ? "&" : "?")
                    + "connectionTimeZone=UTC&forceConnectionTimeZoneToSession=true";
        }
        dataSource.setUrl(jdbcUrl);
        dataSource.setUsername(container.getUsername());
        dataSource.setPassword(container.getPassword());
        dataSource.setDriverClassName(container.getDriverClassName());
        return dataSource;
    }

    private static AsyncTaskEnvelope envelope(UUID taskId) {
        return new AsyncTaskEnvelope(
                taskId,
                "resume-events",
                "resume.parse",
                1,
                "{\"resumeId\":42}",
                AsyncTaskMessageValidator.calculatePayloadHash("{\"resumeId\":42}"),
                "resume.parse:42",
                "resume",
                "42",
                Map.of("trace-id", "trace-42"),
                NOW);
    }

    private record TestOutboxClaim(String leaseToken, int attempt, AsyncTaskEnvelope envelope) {
    }

    private static final class InMemoryEnvelopeSerializer implements TaskPayloadSerializer {
        /**
         * 按任务 ID 保存的信封。
         */
        private final Map<String, AsyncTaskEnvelope> envelopes = new ConcurrentHashMap<>();

        @Override
        public String serialize(Object payload) {
            if (payload instanceof AsyncTaskEnvelope envelope) {
                String key = envelope.taskId().toString();
                envelopes.put(key, envelope);
                return key;
            }
            return payload.toString();
        }

        @Override
        public <T> T deserialize(String payloadJson, Class<T> payloadType) {
            return payloadType.cast(envelopes.get(payloadJson));
        }

        private AsyncTaskEnvelope envelope(UUID taskId) {
            return envelopes.get(taskId.toString());
        }
    }
}
