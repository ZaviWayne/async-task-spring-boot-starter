package com.zaviwayne.asynctask.jdbc;

import com.zaviwayne.asynctask.core.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class JdbcTaskStoreTest {
    private static final Instant NOW = Instant.parse("2026-08-26T00:00:00Z");

    private DriverManagerDataSource dataSource;

    private JdbcTaskStore taskStore;

    private TransactionTemplate transactionTemplate;

    private TaskPayloadSerializer serializer;

    private JdbcTemplate jdbcTemplate;

    @BeforeEach
    void setUp() {
        dataSource = new DriverManagerDataSource(
                "jdbc:h2:mem:async_task_" + UUID.randomUUID() + ";MODE=PostgreSQL;DB_CLOSE_DELAY=-1");
        dataSource.setDriverClassName("org.h2.Driver");
        jdbcTemplate = new JdbcTemplate(dataSource);
        createSchema(jdbcTemplate);
        DataSourceTransactionManager transactionManager = new DataSourceTransactionManager(dataSource);
        transactionTemplate = new TransactionTemplate(transactionManager);
        serializer = new InMemoryEnvelopeSerializer();
        taskStore = new JdbcTaskStore(
                new NamedParameterJdbcTemplate(dataSource),
                transactionManager,
                new H2TestDialect(),
                serializer);
    }

    @Test
    void shouldRecoverExpiredOutboxLeaseAndRejectOldToken() {
        AsyncTaskEnvelope envelope = envelope();
        transactionTemplate.executeWithoutResult(status -> taskStore.saveOutbox(envelope));

        TestOutboxClaim firstClaim = claimFirstOutbox(
                10, 3, NOW, Duration.ofSeconds(10));
        Map<AsyncTaskEnvelope, Integer> activeLeaseClaims = taskStore.claimOutbox(
                10, 3, NOW.plusSeconds(5), Duration.ofSeconds(10), UUID.randomUUID().toString());
        TestOutboxClaim recoveredClaim = claimFirstOutbox(
                10, 3, NOW.plusSeconds(11), Duration.ofSeconds(10));

        assertThat(activeLeaseClaims).isEmpty();
        assertThat(firstClaim.attempt()).isEqualTo(1);
        assertThat(recoveredClaim.attempt()).isEqualTo(2);
        assertThat(recoveredClaim.leaseToken()).isNotEqualTo(firstClaim.leaseToken());
        assertThat(taskStore.markOutboxDispatched(
                envelope.taskId(), firstClaim.leaseToken(), NOW.plusSeconds(12))).isFalse();
        assertThat(taskStore.markOutboxDispatched(
                envelope.taskId(), recoveredClaim.leaseToken(), NOW.plusSeconds(12))).isTrue();
    }

    @Test
    void shouldRecoverExpiredExecutionLeaseAndKeepCompletedTaskIdempotent() {
        AsyncTaskEnvelope envelope = envelope();
        transactionTemplate.executeWithoutResult(status -> taskStore.saveOutbox(envelope));
        TestOutboxClaim dispatchClaim = claimFirstOutbox(
                10, 3, NOW, Duration.ofSeconds(10));
        assertThat(taskStore.markOutboxDispatched(
                envelope.taskId(), dispatchClaim.leaseToken(), NOW.plusSeconds(1))).isTrue();

        String firstLeaseToken = taskStore.claimExecution(
                envelope, NOW.plusSeconds(2), Duration.ofSeconds(10)).orElseThrow();
        assertThatThrownBy(() -> taskStore.claimExecution(
                envelope, NOW.plusSeconds(5), Duration.ofSeconds(10)))
                .isInstanceOf(TaskExecutionInProgressException.class)
                .hasMessage("异步任务正在其他实例执行: " + envelope.taskId());
        String recoveredLeaseToken = taskStore.claimExecution(
                envelope, NOW.plusSeconds(13), Duration.ofSeconds(10)).orElseThrow();

        assertThat(recoveredLeaseToken).isNotEqualTo(firstLeaseToken);
        assertThat(taskStore.markExecutionCompleted(
                envelope.taskId(), firstLeaseToken, NOW.plusSeconds(14))).isFalse();
        assertThat(taskStore.markExecutionCompleted(
                envelope.taskId(), recoveredLeaseToken, NOW.plusSeconds(14))).isTrue();
        assertThat(taskStore.claimExecution(
                envelope, NOW.plusSeconds(15), Duration.ofSeconds(10))).isEmpty();
    }

    @Test
    void shouldRecoverExpiredExecutionLeaseWhenObserverFails() {
        AsyncTaskObserver observer = new AsyncTaskObserverStub() {
            @Override
            public void onExecutionLeaseRecovered(String destination, String taskType) {
                throw new IllegalStateException("测试执行租约接管观测失败");
            }
        };
        JdbcTaskStore observedTaskStore = new JdbcTaskStore(
                new NamedParameterJdbcTemplate(dataSource),
                new DataSourceTransactionManager(dataSource),
                new H2TestDialect(),
                serializer,
                observer);
        AsyncTaskEnvelope envelope = envelope();
        transactionTemplate.executeWithoutResult(status -> observedTaskStore.saveOutbox(envelope));
        String dispatchLeaseToken = UUID.randomUUID().toString();
        assertThat(observedTaskStore.claimOutbox(
                1, 3, NOW, Duration.ofSeconds(10), dispatchLeaseToken)).hasSize(1);
        assertThat(observedTaskStore.markOutboxDispatched(
                envelope.taskId(), dispatchLeaseToken, NOW.plusSeconds(1))).isTrue();
        String firstLeaseToken = observedTaskStore.claimExecution(
                envelope, NOW.plusSeconds(2), Duration.ofSeconds(10)).orElseThrow();

        String recoveredLeaseToken = observedTaskStore.claimExecution(
                envelope, NOW.plusSeconds(13), Duration.ofSeconds(10)).orElseThrow();

        assertThat(recoveredLeaseToken).isNotEqualTo(firstLeaseToken);
        assertThat(observedTaskStore.markExecutionCompleted(
                envelope.taskId(), recoveredLeaseToken, NOW.plusSeconds(14))).isTrue();
    }

    @Test
    void shouldRecoverExpiredFinalDispatchLeaseWithoutTerminalFailure() {
        AtomicInteger observedTerminalFailures = new AtomicInteger();
        AsyncTaskObserver observer = new AsyncTaskObserverStub() {
            @Override
            public void onDispatchFailed(String destination, String taskType, boolean terminal) {
                if (terminal) {
                    observedTerminalFailures.incrementAndGet();
                }
            }
        };
        JdbcTaskStore observedTaskStore = new JdbcTaskStore(
                new NamedParameterJdbcTemplate(dataSource),
                new DataSourceTransactionManager(dataSource),
                new H2TestDialect(),
                serializer,
                observer);
        AsyncTaskEnvelope envelope = envelope();
        transactionTemplate.executeWithoutResult(status -> observedTaskStore.saveOutbox(envelope));
        String firstLeaseToken = UUID.randomUUID().toString();
        assertThat(observedTaskStore.claimOutbox(
                1, 1, NOW, Duration.ofSeconds(1), firstLeaseToken)).hasSize(1);
        String recoveredLeaseToken = UUID.randomUUID().toString();

        Map<AsyncTaskEnvelope, Integer> recovered = observedTaskStore.claimOutbox(
                1, 1, NOW.plusSeconds(2), Duration.ofSeconds(1), recoveredLeaseToken);

        assertThat(recovered).containsEntry(envelope, 2);
        assertThat(observedTerminalFailures).hasValue(0);
        assertThat(observedTaskStore.markOutboxDispatched(
                envelope.taskId(), recoveredLeaseToken, NOW.plusSeconds(3))).isTrue();
    }

    @Test
    void shouldRetryUncertainDeliveryBeyondMaximumAttempts() {
        AsyncTaskEnvelope envelope = envelope();
        transactionTemplate.executeWithoutResult(status -> taskStore.saveOutbox(envelope));
        AtomicInteger sendAttempts = new AtomicInteger();
        AsyncTaskTransport transport = ignored -> {
            sendAttempts.incrementAndGet();
            throw new AsyncTaskTransportException(
                    "等待 Kafka 发送确认超时", new java.util.concurrent.TimeoutException(), true);
        };
        ExponentialBackoffPolicy retryPolicy = new ExponentialBackoffPolicy(
                1, Duration.ofSeconds(1), Duration.ofMinutes(1));
        AsyncTaskDispatcher firstDispatcher = new AsyncTaskDispatcher(
                taskStore, transport, retryPolicy,
                Clock.fixed(NOW, java.time.ZoneOffset.UTC), 1, Duration.ofSeconds(10));
        AsyncTaskDispatcher secondDispatcher = new AsyncTaskDispatcher(
                taskStore, transport, retryPolicy,
                Clock.fixed(NOW.plusSeconds(2), java.time.ZoneOffset.UTC), 1, Duration.ofSeconds(10));

        firstDispatcher.dispatchBatch();
        secondDispatcher.dispatchBatch();

        AsyncTaskInfo taskInfo = taskStore.findByTaskId(envelope.taskId()).orElseThrow();
        assertThat(sendAttempts).hasValue(2);
        assertThat(taskInfo.status()).isEqualTo(AsyncTaskStatus.RETRY);
        assertThat(taskInfo.dispatchAttempts()).isEqualTo(2);
    }

    @Test
    void shouldKeepRecoveredDispatchLeaseUncertainAfterExplicitFailures() {
        AsyncTaskEnvelope envelope = envelope();
        transactionTemplate.executeWithoutResult(status -> taskStore.saveOutbox(envelope));
        assertThat(taskStore.claimOutbox(
                1, 1, NOW, Duration.ofSeconds(1), UUID.randomUUID().toString())).hasSize(1);
        AtomicInteger sendAttempts = new AtomicInteger();
        AsyncTaskTransport transport = ignored -> {
            sendAttempts.incrementAndGet();
            throw new IllegalStateException("Kafka 明确拒绝发送");
        };
        ExponentialBackoffPolicy retryPolicy = new ExponentialBackoffPolicy(
                1, Duration.ofSeconds(1), Duration.ofMinutes(1));
        AsyncTaskDispatcher recoveredDispatcher = new AsyncTaskDispatcher(
                taskStore, transport, retryPolicy,
                Clock.fixed(NOW.plusSeconds(2), java.time.ZoneOffset.UTC), 1, Duration.ofSeconds(1));
        AsyncTaskDispatcher uncertainDispatcher = new AsyncTaskDispatcher(
                taskStore, transport, retryPolicy,
                Clock.fixed(NOW.plusSeconds(4), java.time.ZoneOffset.UTC), 1, Duration.ofSeconds(1));

        recoveredDispatcher.dispatchBatch();
        uncertainDispatcher.dispatchBatch();

        AsyncTaskInfo taskInfo = taskStore.findByTaskId(envelope.taskId()).orElseThrow();
        assertThat(sendAttempts).hasValue(2);
        assertThat(taskInfo.status()).isEqualTo(AsyncTaskStatus.RETRY);
        assertThat(taskInfo.dispatchAttempts()).isEqualTo(3);
    }

    @Test
    void shouldKeepDispatchingWhenStatusUpdateFailsAfterSuccessfulSend() {
        AsyncTaskEnvelope envelope = envelope();
        transactionTemplate.executeWithoutResult(status -> taskStore.saveOutbox(envelope));
        AtomicInteger dispatchFailures = new AtomicInteger();
        AsyncTaskObserver observer = new AsyncTaskObserverStub() {
            @Override
            public void onDispatchFailed(String destination, String taskType, boolean terminal) {
                dispatchFailures.incrementAndGet();
            }
        };
        AsyncTaskTransport transport = ignored -> jdbcTemplate.execute("""
                ALTER TABLE async_task_outbox
                ADD CONSTRAINT block_dispatched_status CHECK (status <> %d)
                """.formatted(OutboxStatus.DISPATCHED.getCode()));
        AsyncTaskDispatcher dispatcher = new AsyncTaskDispatcher(
                taskStore,
                transport,
                new ExponentialBackoffPolicy(1, Duration.ofSeconds(1), Duration.ofMinutes(1)),
                Clock.fixed(NOW, java.time.ZoneOffset.UTC),
                1,
                Duration.ofSeconds(10),
                observer);

        dispatcher.dispatchBatch();

        AsyncTaskInfo taskInfo = taskStore.findByTaskId(envelope.taskId()).orElseThrow();
        assertThat(taskInfo.status()).isEqualTo(AsyncTaskStatus.DISPATCHING);
        assertThat(taskInfo.dispatchAttempts()).isEqualTo(1);
        assertThat(dispatchFailures).hasValue(0);
    }

    @Test
    void shouldNotObserveRecoveredDispatchLeasesWhenClaimTransactionRollsBack() {
        AsyncTaskEnvelope firstEnvelope = envelope("resume.parse:rollback:1");
        AsyncTaskEnvelope secondEnvelope = envelope("resume.parse:rollback:2");
        transactionTemplate.executeWithoutResult(status -> taskStore.saveOutbox(firstEnvelope));
        transactionTemplate.executeWithoutResult(status -> taskStore.saveOutbox(secondEnvelope));
        assertThat(taskStore.claimOutbox(
                2, 3, NOW, Duration.ofSeconds(1), UUID.randomUUID().toString())).hasSize(2);
        AtomicInteger recoveredLeases = new AtomicInteger();
        AsyncTaskObserver observer = new AsyncTaskObserverStub() {
            @Override
            public void onDispatchLeaseRecovered(String destination, String taskType) {
                recoveredLeases.incrementAndGet();
            }
        };
        JdbcTaskStore failingTaskStore = new JdbcTaskStore(
                new NamedParameterJdbcTemplate(dataSource),
                new DataSourceTransactionManager(dataSource),
                new SecondUuidFailingDialect(),
                serializer,
                observer);

        assertThatThrownBy(() -> failingTaskStore.claimOutbox(
                2, 3, NOW.plusSeconds(2), Duration.ofSeconds(1), UUID.randomUUID().toString()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("测试第二条租约更新失败");

        assertThat(recoveredLeases).hasValue(0);
        assertThat(taskStore.claimOutbox(
                2, 3, NOW.plusSeconds(2), Duration.ofSeconds(1), UUID.randomUUID().toString()))
                .hasSize(2)
                .allSatisfy((ignored, attempt) -> assertThat(attempt).isEqualTo(2));
    }

    @Test
    void shouldRejectNonPositiveLeaseDurations() {
        assertThatThrownBy(() -> new AsyncTaskDispatcher(
                taskStore,
                ignored -> {
                },
                new ExponentialBackoffPolicy(3, Duration.ofSeconds(1), Duration.ofMinutes(1)),
                Clock.systemUTC(),
                1,
                Duration.ZERO))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("投递租约时长必须大于 0");
        assertThatThrownBy(() -> taskStore.claimOutbox(
                1, 3, NOW, Duration.ZERO, UUID.randomUUID().toString()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("投递租约时长必须大于 0");
        assertThatThrownBy(() -> taskStore.claimExecution(
                envelope(), NOW, Duration.ofSeconds(-1)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("执行租约时长必须大于 0");
        assertThatThrownBy(() -> taskStore.heartbeatExecution(
                UUID.randomUUID(), UUID.randomUUID().toString(), NOW, Duration.ZERO))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("执行租约时长必须大于 0");
        assertThatThrownBy(() -> taskStore.updateExecutionProgress(
                UUID.randomUUID(), UUID.randomUUID().toString(), "{}", NOW, Duration.ofSeconds(-1)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("执行租约时长必须大于 0");
    }

    @Test
    void shouldRejectOversizedEnvelopeAndProgressBeforeWriting() {
        JdbcTaskStore limitedTaskStore = new JdbcTaskStore(
                new NamedParameterJdbcTemplate(dataSource),
                new DataSourceTransactionManager(dataSource),
                new H2TestDialect(),
                serializer,
                NoOpAsyncTaskObserver.INSTANCE,
                new AsyncTaskContentLimits(1, 1));

        assertThatThrownBy(() -> transactionTemplate.executeWithoutResult(
                status -> limitedTaskStore.saveOutbox(envelope())))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("任务信封 JSON 不能超过 1 个 UTF-8 字节，实际为 36 字节");
        assertThatThrownBy(() -> limitedTaskStore.updateExecutionProgress(
                UUID.randomUUID(), UUID.randomUUID().toString(), "{}", NOW, Duration.ofSeconds(1)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("任务进度 JSON 不能超过 1 个 UTF-8 字节，实际为 2 字节");
    }

    @Test
    void shouldRejectSameIdempotencyKeyWithDifferentHeaders() {
        AsyncTaskEnvelope envelope = envelope();
        transactionTemplate.executeWithoutResult(status -> taskStore.saveOutbox(envelope));
        AsyncTaskEnvelope conflictingEnvelope = new AsyncTaskEnvelope(
                UUID.randomUUID(),
                envelope.destination(),
                envelope.taskType(),
                envelope.schemaVersion(),
                envelope.payloadJson(),
                envelope.payloadHash(),
                envelope.idempotencyKey(),
                envelope.referenceType(),
                envelope.referenceId(),
                Map.of("trace-id", "trace-2"),
                NOW.plusSeconds(1));

        assertThatThrownBy(() -> transactionTemplate.executeWithoutResult(
                status -> taskStore.saveOutbox(conflictingEnvelope)))
                .isInstanceOf(DuplicateAsyncTaskException.class)
                .hasMessage("幂等键已被不同的异步任务使用: " + envelope.idempotencyKey());
    }

    @Test
    void shouldRejectChangedPayloadAtEnvelopeBoundaryWhenHashIsCopied() {
        AsyncTaskEnvelope envelope = envelope();

        assertThatThrownBy(() -> new AsyncTaskEnvelope(
                envelope.taskId(),
                envelope.destination(),
                envelope.taskType(),
                envelope.schemaVersion(),
                "{\"resumeId\":2}",
                envelope.payloadHash(),
                envelope.idempotencyKey(),
                envelope.referenceType(),
                envelope.referenceId(),
                envelope.headers(),
                envelope.createdAt()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("载荷摘要与 JSON 载荷内容不一致");
    }

    @Test
    void shouldIgnoreDuplicateAndStaleDeadLetters() {
        AsyncTaskEnvelope envelope = envelope();
        transactionTemplate.executeWithoutResult(status -> taskStore.saveOutbox(envelope));
        TestOutboxClaim dispatchClaim = claimFirstOutbox(
                10, 3, NOW, Duration.ofSeconds(10));
        assertThat(taskStore.markOutboxDispatched(
                envelope.taskId(), dispatchClaim.leaseToken(), NOW.plusSeconds(1))).isTrue();
        String executionLeaseToken = taskStore.claimExecution(
                envelope, NOW.plusSeconds(2), Duration.ofSeconds(10)).orElseThrow();
        assertThat(taskStore.markExecutionFailed(
                envelope.taskId(), executionLeaseToken, "测试执行失败", NOW.plusSeconds(3))).isTrue();

        AsyncTaskEnvelope conflictingEnvelope = new AsyncTaskEnvelope(
                envelope.taskId(),
                envelope.destination(),
                envelope.taskType(),
                envelope.schemaVersion(),
                "{\"resumeId\":2}",
                AsyncTaskMessageValidator.calculatePayloadHash("{\"resumeId\":2}"),
                envelope.idempotencyKey(),
                envelope.referenceType(),
                envelope.referenceId(),
                envelope.headers(),
                envelope.createdAt());
        assertThatThrownBy(() -> taskStore.markOutboxDead(
                conflictingEnvelope, "错误死信", NOW.plusSeconds(4)))
                .isInstanceOf(DuplicateAsyncTaskException.class)
                .hasMessage("Outbox 记录与消费消息内容不一致: " + envelope.idempotencyKey());

        assertThat(taskStore.markOutboxDead(
                envelope, "测试死信", NOW.plusSeconds(4))).isTrue();
        assertThat(taskStore.markOutboxDead(
                envelope, "重复死信", NOW.plusSeconds(5))).isFalse();
        assertThat(taskStore.findByTaskId(envelope.taskId()).orElseThrow().completedAt())
                .isEqualTo(NOW.plusSeconds(4));

        assertThat(taskStore.requeue(envelope.taskId(), NOW.plusSeconds(6)))
                .isEqualTo(AsyncTaskRequeueResult.REQUEUED);
        assertThat(taskStore.markOutboxDead(
                envelope, "延迟死信", NOW.plusSeconds(7))).isFalse();
        TestOutboxClaim requeuedClaim = claimFirstOutbox(
                10, 3, NOW.plusSeconds(8), Duration.ofSeconds(10));
        AsyncTaskEnvelope requeuedEnvelope = requeuedClaim.envelope();
        assertThat(requeuedEnvelope.generation()).isEqualTo(1);
        assertThat(taskStore.markOutboxDispatched(
                envelope.taskId(), requeuedClaim.leaseToken(), NOW.plusSeconds(9))).isTrue();
        String requeuedExecutionLease = taskStore.claimExecution(
                requeuedEnvelope, NOW.plusSeconds(10), Duration.ofSeconds(10)).orElseThrow();
        assertThat(taskStore.markExecutionFailed(
                envelope.taskId(), requeuedExecutionLease, "新一轮执行失败", NOW.plusSeconds(11))).isTrue();

        assertThat(taskStore.markOutboxDead(
                envelope, "旧代死信", NOW.plusSeconds(12))).isFalse();
        assertThat(taskStore.findByTaskId(envelope.taskId()).orElseThrow().status())
                .isEqualTo(AsyncTaskStatus.FAILED);
        assertThat(taskStore.markOutboxDead(
                requeuedEnvelope, "新代死信", NOW.plusSeconds(13))).isTrue();
        assertThat(taskStore.markOutboxDead(
                envelope(), "记录不存在", NOW.plusSeconds(14))).isFalse();
    }

    @Test
    void shouldTreatFailureFromExpiredExecutionLeaseAsLeaseConflict() {
        AsyncTaskEnvelope envelope = envelope();
        transactionTemplate.executeWithoutResult(status -> taskStore.saveOutbox(envelope));
        TestOutboxClaim dispatchClaim = claimFirstOutbox(
                10, 3, NOW, Duration.ofSeconds(10));
        assertThat(taskStore.markOutboxDispatched(
                envelope.taskId(), dispatchClaim.leaseToken(), NOW.plusSeconds(1))).isTrue();
        LeaseLosingHandler handler = new LeaseLosingHandler(taskStore, envelope);
        AsyncTaskHandlerRegistry registry = new AsyncTaskHandlerRegistry(List.of(handler));

        try (AsyncTaskProcessor processor = new AsyncTaskProcessor(
                taskStore,
                registry,
                serializer,
                Clock.fixed(NOW.plusSeconds(2), java.time.ZoneOffset.UTC),
                Duration.ofSeconds(1),
                Duration.ofMillis(900))) {
            assertThatThrownBy(() -> processor.process(envelope))
                    .isInstanceOf(TaskExecutionInProgressException.class)
                    .hasMessage("无法记录异步任务执行失败，执行租约已经失效: " + envelope.taskId())
                    .satisfies(exception -> assertThat(exception.getSuppressed())
                            .singleElement()
                            .isInstanceOf(IllegalStateException.class));
        }

        assertThat(taskStore.markExecutionCompleted(
                envelope.taskId(), handler.recoveredLeaseToken, NOW.plusSeconds(5))).isTrue();
    }

    @Test
    void shouldMarkMissingHandlerAsFailedBeforeDeadLetter() {
        AsyncTaskEnvelope envelope = envelope();
        transactionTemplate.executeWithoutResult(status -> taskStore.saveOutbox(envelope));
        TestOutboxClaim dispatchClaim = claimFirstOutbox(
                10, 3, NOW, Duration.ofSeconds(10));
        assertThat(taskStore.markOutboxDispatched(
                envelope.taskId(), dispatchClaim.leaseToken(), NOW.plusSeconds(1))).isTrue();
        AsyncTaskHandlerRegistry registry = new AsyncTaskHandlerRegistry(List.of());

        try (AsyncTaskProcessor processor = new AsyncTaskProcessor(
                taskStore,
                registry,
                serializer,
                Clock.fixed(NOW.plusSeconds(2), java.time.ZoneOffset.UTC),
                Duration.ofMinutes(1),
                Duration.ofSeconds(10))) {
            assertThatThrownBy(() -> processor.process(envelope))
                    .isInstanceOf(AsyncTaskProcessingException.class)
                    .hasMessage("异步任务执行失败: " + envelope.taskId())
                    .hasCauseInstanceOf(NoAsyncTaskHandlerException.class);
            assertThat(taskStore.findByTaskId(envelope.taskId()).orElseThrow().status())
                    .isEqualTo(AsyncTaskStatus.FAILED);

            processor.processDeadLetter(envelope, "处理器不存在");
        }

        assertThat(taskStore.findByTaskId(envelope.taskId()).orElseThrow().status())
                .isEqualTo(AsyncTaskStatus.DEAD);
    }

    @Test
    void shouldCompleteExecutionWhenObserverFails() {
        AsyncTaskEnvelope envelope = envelope();
        transactionTemplate.executeWithoutResult(status -> taskStore.saveOutbox(envelope));
        TestOutboxClaim dispatchClaim = claimFirstOutbox(
                10, 3, NOW, Duration.ofSeconds(10));
        assertThat(taskStore.markOutboxDispatched(
                envelope.taskId(), dispatchClaim.leaseToken(), NOW.plusSeconds(1))).isTrue();
        AsyncTaskObserver observer = new AsyncTaskObserverStub() {
            @Override
            public void onExecutionSucceeded(String destination, String taskType) {
                throw new IllegalStateException("测试执行成功观测失败");
            }
        };
        AsyncTaskHandlerRegistry registry = new AsyncTaskHandlerRegistry(List.of(new ProgressHandler()));

        try (AsyncTaskProcessor processor = new AsyncTaskProcessor(
                taskStore,
                registry,
                serializer,
                Clock.fixed(NOW.plusSeconds(2), java.time.ZoneOffset.UTC),
                Duration.ofMinutes(1),
                Duration.ofSeconds(10),
                observer)) {
            assertThat(processor.process(envelope)).isTrue();
        }

        assertThat(taskStore.findByTaskId(envelope.taskId()).orElseThrow().status())
                .isEqualTo(AsyncTaskStatus.SUCCESS);
    }

    @Test
    void shouldNotObserveDispatchFailureAfterLeaseRecovery() {
        AsyncTaskEnvelope envelope = envelope();
        transactionTemplate.executeWithoutResult(status -> taskStore.saveOutbox(envelope));
        AtomicInteger observedFailures = new AtomicInteger();
        AsyncTaskObserver observer = new AsyncTaskObserverStub() {
            @Override
            public void onDispatchFailed(String destination, String taskType, boolean terminal) {
                observedFailures.incrementAndGet();
            }
        };
        AsyncTaskTransport transport = ignored -> {
            Map<AsyncTaskEnvelope, Integer> recoveredClaims = taskStore.claimOutbox(
                    1, 3, NOW.plusSeconds(2), Duration.ofSeconds(10), UUID.randomUUID().toString());
            assertThat(recoveredClaims).hasSize(1);
            throw new IllegalStateException("测试投递失败");
        };
        AsyncTaskDispatcher dispatcher = new AsyncTaskDispatcher(
                taskStore,
                transport,
                new ExponentialBackoffPolicy(3, Duration.ofSeconds(1), Duration.ofMinutes(1)),
                Clock.fixed(NOW, java.time.ZoneOffset.UTC),
                1,
                Duration.ofSeconds(1),
                observer);

        dispatcher.dispatchBatch();

        assertThat(observedFailures).hasValue(0);
        assertThat(taskStore.findByTaskId(envelope.taskId()).orElseThrow().status())
                .isEqualTo(AsyncTaskStatus.DISPATCHING);
    }

    @Test
    void shouldAllowExecutionBeforeDispatchStatusWriteBack() {
        AsyncTaskEnvelope envelope = envelope();
        transactionTemplate.executeWithoutResult(status -> taskStore.saveOutbox(envelope));
        TestOutboxClaim dispatchClaim = claimFirstOutbox(
                10, 3, NOW, Duration.ofSeconds(10));

        String executionLeaseToken = taskStore.claimExecution(
                envelope, NOW.plusSeconds(1), Duration.ofSeconds(10)).orElseThrow();

        assertThat(taskStore.markOutboxDispatched(
                envelope.taskId(), dispatchClaim.leaseToken(), NOW.plusSeconds(2))).isFalse();
        assertThat(taskStore.markExecutionCompleted(
                envelope.taskId(), executionLeaseToken, NOW.plusSeconds(3))).isTrue();
    }

    @Test
    void shouldAutomaticallyHeartbeatWhileHandlerRuns() throws Exception {
        AsyncTaskEnvelope envelope = envelope();
        transactionTemplate.executeWithoutResult(status -> taskStore.saveOutbox(envelope));
        Instant dispatchTime = Instant.now();
        TestOutboxClaim dispatchClaim = claimFirstOutbox(
                10, 3, dispatchTime, Duration.ofSeconds(1));
        assertThat(taskStore.markOutboxDispatched(
                envelope.taskId(), dispatchClaim.leaseToken(), dispatchTime)).isTrue();
        BlockingHandler handler = new BlockingHandler();
        AsyncTaskHandlerRegistry registry = new AsyncTaskHandlerRegistry(List.of(handler));

        try (AsyncTaskProcessor processor = new AsyncTaskProcessor(
                taskStore,
                registry,
                serializer,
                Clock.systemUTC(),
                Duration.ofMillis(120),
                Duration.ofMillis(20))) {
            CompletableFuture<Boolean> processing = CompletableFuture.supplyAsync(
                    () -> processor.process(envelope));
            try {
                assertThat(handler.started.await(5, TimeUnit.SECONDS)).isTrue();
                TimeUnit.MILLISECONDS.sleep(250);

                assertThatThrownBy(() -> taskStore.claimExecution(
                        envelope, Instant.now(), Duration.ofMillis(120)))
                        .isInstanceOf(TaskExecutionInProgressException.class);
            } finally {
                handler.release.countDown();
            }
            assertThat(processing.get(5, TimeUnit.SECONDS)).isTrue();
        }
    }

    @Test
    void shouldHeartbeatConcurrentExecutionsWithConfiguredThreadPool() throws Exception {
        AsyncTaskEnvelope firstEnvelope = envelope();
        AsyncTaskEnvelope secondEnvelope = new AsyncTaskEnvelope(
                UUID.randomUUID(),
                firstEnvelope.destination(),
                firstEnvelope.taskType(),
                firstEnvelope.schemaVersion(),
                firstEnvelope.payloadJson(),
                firstEnvelope.payloadHash(),
                "resume.parse:2",
                firstEnvelope.referenceType(),
                firstEnvelope.referenceId(),
                firstEnvelope.headers(),
                firstEnvelope.createdAt());
        transactionTemplate.executeWithoutResult(status -> taskStore.saveOutbox(firstEnvelope));
        transactionTemplate.executeWithoutResult(status -> taskStore.saveOutbox(secondEnvelope));
        Instant dispatchTime = Instant.now();
        String dispatchLeaseToken = UUID.randomUUID().toString();
        assertThat(taskStore.claimOutbox(
                10, 3, dispatchTime, Duration.ofSeconds(1), dispatchLeaseToken)).hasSize(2);
        assertThat(taskStore.markOutboxDispatched(
                firstEnvelope.taskId(), dispatchLeaseToken, dispatchTime)).isTrue();
        assertThat(taskStore.markOutboxDispatched(
                secondEnvelope.taskId(), dispatchLeaseToken, dispatchTime)).isTrue();
        BlockingHandler handler = new BlockingHandler(2);
        AsyncTaskHandlerRegistry registry = new AsyncTaskHandlerRegistry(List.of(handler));

        try (AsyncTaskProcessor processor = new AsyncTaskProcessor(
                taskStore,
                registry,
                serializer,
                Clock.systemUTC(),
                Duration.ofMillis(120),
                Duration.ofMillis(20),
                2,
                NoOpAsyncTaskObserver.INSTANCE)) {
            CompletableFuture<Boolean> firstProcessing = CompletableFuture.supplyAsync(
                    () -> processor.process(firstEnvelope));
            CompletableFuture<Boolean> secondProcessing = CompletableFuture.supplyAsync(
                    () -> processor.process(secondEnvelope));
            try {
                assertThat(handler.started.await(5, TimeUnit.SECONDS)).isTrue();
                TimeUnit.MILLISECONDS.sleep(250);

                assertThatThrownBy(() -> taskStore.claimExecution(
                        firstEnvelope, Instant.now(), Duration.ofMillis(120)))
                        .isInstanceOf(TaskExecutionInProgressException.class);
                assertThatThrownBy(() -> taskStore.claimExecution(
                        secondEnvelope, Instant.now(), Duration.ofMillis(120)))
                        .isInstanceOf(TaskExecutionInProgressException.class);
            } finally {
                handler.release.countDown();
            }
            assertThat(firstProcessing.get(5, TimeUnit.SECONDS)).isTrue();
            assertThat(secondProcessing.get(5, TimeUnit.SECONDS)).isTrue();
        }
    }

    @Test
    void shouldNotObserveDuplicateEnqueueWhenInsertRaisesDuplicateKey() {
        AtomicInteger observedEnqueues = new AtomicInteger();
        AsyncTaskObserver observer = new AsyncTaskObserverStub() {
            @Override
            public void onEnqueued(String destination, String taskType) {
                observedEnqueues.incrementAndGet();
            }
        };
        JdbcTaskStore duplicateKeyTaskStore = new JdbcTaskStore(
                new NamedParameterJdbcTemplate(dataSource),
                new DataSourceTransactionManager(dataSource),
                new DuplicateKeyH2TestDialect(),
                serializer,
                observer);
        AsyncTaskEnvelope firstEnvelope = envelope();
        AsyncTaskEnvelope duplicateEnvelope = new AsyncTaskEnvelope(
                UUID.randomUUID(),
                firstEnvelope.destination(),
                firstEnvelope.taskType(),
                firstEnvelope.schemaVersion(),
                firstEnvelope.payloadJson(),
                firstEnvelope.payloadHash(),
                firstEnvelope.idempotencyKey(),
                firstEnvelope.referenceType(),
                firstEnvelope.referenceId(),
                firstEnvelope.headers(),
                firstEnvelope.createdAt());

        UUID firstTaskId = transactionTemplate.execute(
                status -> duplicateKeyTaskStore.saveOutbox(firstEnvelope));
        UUID duplicateTaskId = transactionTemplate.execute(
                status -> duplicateKeyTaskStore.saveOutbox(duplicateEnvelope));

        assertThat(firstTaskId).isEqualTo(firstEnvelope.taskId());
        assertThat(duplicateTaskId).isEqualTo(firstEnvelope.taskId());
        assertThat(observedEnqueues).hasValue(1);
    }

    @Test
    void shouldPersistProgressReportedByHandler() {
        AsyncTaskEnvelope envelope = envelope();
        transactionTemplate.executeWithoutResult(status -> taskStore.saveOutbox(envelope));
        TestOutboxClaim dispatchClaim = claimFirstOutbox(
                10, 3, NOW, Duration.ofSeconds(10));
        assertThat(taskStore.markOutboxDispatched(
                envelope.taskId(), dispatchClaim.leaseToken(), NOW.plusSeconds(1))).isTrue();
        AsyncTaskHandlerRegistry registry = new AsyncTaskHandlerRegistry(List.of(new ProgressHandler()));

        try (AsyncTaskProcessor processor = new AsyncTaskProcessor(
                taskStore,
                registry,
                serializer,
                Clock.fixed(NOW.plusSeconds(2), java.time.ZoneOffset.UTC),
                Duration.ofMinutes(1),
                Duration.ofSeconds(10))) {
            assertThat(processor.process(envelope)).isTrue();
        }

        String progressJson = jdbcTemplate.queryForObject(
                "SELECT progress_json FROM async_task_outbox WHERE task_id = ?",
                String.class,
                envelope.taskId().toString());
        assertThat(progressJson).isEqualTo("Progress[completed=10, total=20]");
    }

    @Test
    void shouldQueryReferencedTaskAndRequeueTerminalTask() {
        AsyncTaskEnvelope envelope = referencedEnvelope();
        transactionTemplate.executeWithoutResult(status -> taskStore.saveOutbox(envelope));
        TestOutboxClaim dispatchClaim = claimFirstOutbox(
                10, 3, NOW, Duration.ofSeconds(10));
        assertThat(taskStore.markOutboxDispatched(
                envelope.taskId(), dispatchClaim.leaseToken(), NOW.plusSeconds(1))).isTrue();
        String executionLeaseToken = taskStore.claimExecution(
                envelope, NOW.plusSeconds(2), Duration.ofSeconds(10)).orElseThrow();
        assertThat(taskStore.markExecutionCompleted(
                envelope.taskId(), executionLeaseToken, NOW.plusSeconds(3))).isTrue();
        JdbcAsyncTaskAdmin taskAdmin = new JdbcAsyncTaskAdmin(
                taskStore, Clock.fixed(NOW.plusSeconds(4), java.time.ZoneOffset.UTC));

        AsyncTaskInfo taskInfo = taskAdmin.findByIdempotencyKey(envelope.idempotencyKey()).orElseThrow();

        assertThat(taskInfo.status()).isEqualTo(AsyncTaskStatus.SUCCESS);
        assertThat(taskInfo.referenceType()).isEqualTo("resume");
        assertThat(taskInfo.referenceId()).isEqualTo("1");
        assertThat(taskAdmin.findByReference("resume", "1", 20, 0))
                .extracting(AsyncTaskInfo::taskId)
                .containsExactly(envelope.taskId());
        assertThat(taskAdmin.requeue(envelope.taskId())).isEqualTo(AsyncTaskRequeueResult.REQUEUED);
        assertThat(taskAdmin.findByTaskId(envelope.taskId()).orElseThrow().status())
                .isEqualTo(AsyncTaskStatus.PENDING);
        assertThat(taskAdmin.requeue(envelope.taskId())).isEqualTo(AsyncTaskRequeueResult.NOT_TERMINAL);
        assertThat(taskAdmin.requeue(UUID.randomUUID())).isEqualTo(AsyncTaskRequeueResult.NOT_FOUND);
    }

    @Test
    void shouldCleanTerminalTasksInBatches() {
        AsyncTaskEnvelope firstEnvelope = envelope();
        AsyncTaskEnvelope secondEnvelope = new AsyncTaskEnvelope(
                UUID.randomUUID(),
                firstEnvelope.destination(),
                firstEnvelope.taskType(),
                firstEnvelope.schemaVersion(),
                firstEnvelope.payloadJson(),
                firstEnvelope.payloadHash(),
                "resume.parse:2",
                null,
                null,
                firstEnvelope.headers(),
                NOW);
        transactionTemplate.executeWithoutResult(status -> taskStore.saveOutbox(firstEnvelope));
        complete(firstEnvelope, NOW.plusSeconds(1));
        transactionTemplate.executeWithoutResult(status -> taskStore.saveOutbox(secondEnvelope));
        complete(secondEnvelope, NOW.plusSeconds(2));

        assertThat(taskStore.deleteTerminalTasks(NOW.plusSeconds(10), 1)).isEqualTo(1);
        assertThat(taskStore.deleteTerminalTasks(NOW.plusSeconds(10), 1)).isEqualTo(1);
        assertThat(taskStore.deleteTerminalTasks(NOW.plusSeconds(10), 1)).isZero();
    }

    @Test
    void shouldNotObserveCleanedTasksWhenCleanupTransactionRollsBack() {
        AsyncTaskEnvelope envelope = envelope();
        transactionTemplate.executeWithoutResult(status -> taskStore.saveOutbox(envelope));
        complete(envelope, NOW.plusSeconds(1));
        AtomicInteger cleanedTasks = new AtomicInteger();
        AsyncTaskObserver observer = new AsyncTaskObserverStub() {
            @Override
            public void onCleaned(int count) {
                cleanedTasks.addAndGet(count);
            }
        };
        PlatformTransactionManager transactionManager = new RollbackOnCommitTransactionManager(
                new DataSourceTransactionManager(dataSource));
        JdbcTaskStore observedTaskStore = new JdbcTaskStore(
                new NamedParameterJdbcTemplate(dataSource),
                transactionManager,
                new H2TestDialect(),
                serializer,
                observer);

        assertThat(observedTaskStore.deleteTerminalTasks(NOW.plusSeconds(10), 1)).isEqualTo(1);

        assertThat(cleanedTasks).hasValue(0);
        assertThat(observedTaskStore.findByTaskId(envelope.taskId())).isPresent();
    }

    private void complete(AsyncTaskEnvelope envelope, Instant completedAt) {
        TestOutboxClaim dispatchClaim = claimFirstOutbox(
                1, 3, completedAt, Duration.ofSeconds(10));
        taskStore.markOutboxDispatched(envelope.taskId(), dispatchClaim.leaseToken(), completedAt);
        String executionLeaseToken = taskStore.claimExecution(
                envelope, completedAt, Duration.ofSeconds(10)).orElseThrow();
        taskStore.markExecutionCompleted(envelope.taskId(), executionLeaseToken, completedAt);
    }

    private TestOutboxClaim claimFirstOutbox(int batchSize,
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

    private static AsyncTaskEnvelope envelope() {
        return envelope("resume.parse:1");
    }

    private static AsyncTaskEnvelope envelope(String idempotencyKey) {
        return new AsyncTaskEnvelope(
                UUID.randomUUID(),
                "task-events",
                "resume.parse",
                1,
                "{\"resumeId\":1}",
                AsyncTaskMessageValidator.calculatePayloadHash("{\"resumeId\":1}"),
                idempotencyKey,
                null,
                null,
                Map.of("trace-id", "trace-1"),
                NOW);
    }

    private static AsyncTaskEnvelope referencedEnvelope() {
        return new AsyncTaskEnvelope(
                UUID.randomUUID(),
                "task-events",
                "resume.parse",
                1,
                "{\"resumeId\":1}",
                AsyncTaskMessageValidator.calculatePayloadHash("{\"resumeId\":1}"),
                "resume.parse:1",
                "resume",
                "1",
                Map.of("trace-id", "trace-1"),
                NOW);
    }

    private record TestOutboxClaim(String leaseToken, int attempt, AsyncTaskEnvelope envelope) {
    }

    private static void createSchema(JdbcTemplate jdbcTemplate) {
        jdbcTemplate.execute("""
                CREATE TABLE async_task_outbox (
                    task_id VARCHAR(36) PRIMARY KEY,
                    destination VARCHAR(255) NOT NULL,
                    task_type VARCHAR(200) NOT NULL,
                    schema_version INTEGER NOT NULL,
                    idempotency_key VARCHAR(255) NOT NULL UNIQUE,
                    reference_type VARCHAR(100),
                    reference_id VARCHAR(255),
                    payload_hash VARCHAR(128) NOT NULL,
                    envelope_json CLOB NOT NULL,
                    status SMALLINT NOT NULL,
                    dispatch_attempt INTEGER NOT NULL,
                    execution_attempt INTEGER NOT NULL DEFAULT 0,
                    generation INTEGER NOT NULL DEFAULT 0,
                    next_attempt_at TIMESTAMP,
                    lease_token VARCHAR(36),
                    lease_until TIMESTAMP,
                    last_error VARCHAR(2000),
                    progress_json CLOB,
                    dispatched_at TIMESTAMP,
                    completed_at TIMESTAMP,
                    created_at TIMESTAMP NOT NULL,
                    updated_at TIMESTAMP NOT NULL
                )
                """);
    }

    private static final class H2TestDialect implements AsyncTaskJdbcDialect {
        @Override
        public String databaseProductName() {
            return "H2";
        }

        @Override
        public String insertOutboxSql() {
            return """
                    INSERT INTO async_task_outbox (
                        task_id, destination, task_type, schema_version, idempotency_key,
                        reference_type, reference_id, payload_hash, envelope_json, generation,
                        status, dispatch_attempt, created_at, updated_at
                    ) SELECT :taskId, :destination, :taskType, :schemaVersion, :idempotencyKey,
                             :referenceType, :referenceId, :payloadHash, :envelopeJson, :generation,
                             :status, 0, :now, :now
                      WHERE NOT EXISTS (
                          SELECT 1 FROM async_task_outbox WHERE idempotency_key = :idempotencyKey
                      )
                    """;
        }

        @Override
        public Object uuidParameter(String value) {
            return value;
        }

        @Override
        public String schemaResource() {
            return "unused";
        }
    }

    private static final class DuplicateKeyH2TestDialect implements AsyncTaskJdbcDialect {
        private final AsyncTaskJdbcDialect delegate = new H2TestDialect();

        @Override
        public String databaseProductName() {
            return delegate.databaseProductName();
        }

        @Override
        public String insertOutboxSql() {
            return """
                    INSERT INTO async_task_outbox (
                        task_id, destination, task_type, schema_version, idempotency_key,
                        reference_type, reference_id, payload_hash, envelope_json, generation,
                        status, dispatch_attempt, created_at, updated_at
                    ) VALUES (
                        :taskId, :destination, :taskType, :schemaVersion, :idempotencyKey,
                        :referenceType, :referenceId, :payloadHash, :envelopeJson, :generation,
                        :status, 0, :now, :now
                    )
                    """;
        }

        @Override
        public Object uuidParameter(String value) {
            return delegate.uuidParameter(value);
        }

        @Override
        public String schemaResource() {
            return delegate.schemaResource();
        }
    }

    private static final class SecondUuidFailingDialect implements AsyncTaskJdbcDialect {
        private static final int FAILING_CALL_INDEX = 2;

        private final AsyncTaskJdbcDialect delegate = new H2TestDialect();

        private int uuidCalls;

        @Override
        public String databaseProductName() {
            return delegate.databaseProductName();
        }

        @Override
        public String insertOutboxSql() {
            return delegate.insertOutboxSql();
        }

        @Override
        public Object uuidParameter(String value) {
            uuidCalls++;
            if (uuidCalls == FAILING_CALL_INDEX) {
                throw new IllegalStateException("测试第二条租约更新失败");
            }
            return delegate.uuidParameter(value);
        }

        @Override
        public String schemaResource() {
            return delegate.schemaResource();
        }
    }

    private static final class InMemoryEnvelopeSerializer implements TaskPayloadSerializer {
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
    }

    private static final class RollbackOnCommitTransactionManager implements PlatformTransactionManager {
        /**
         * 实际 JDBC 事务管理器。
         */
        private final PlatformTransactionManager delegate;

        private RollbackOnCommitTransactionManager(PlatformTransactionManager delegate) {
            this.delegate = delegate;
        }

        @Override
        public TransactionStatus getTransaction(TransactionDefinition definition) {
            return delegate.getTransaction(definition);
        }

        @Override
        public void commit(TransactionStatus status) {
            delegate.rollback(status);
        }

        @Override
        public void rollback(TransactionStatus status) {
            delegate.rollback(status);
        }
    }

    private static final class BlockingHandler implements AsyncTaskHandler<String> {
        private final CountDownLatch started;

        private final CountDownLatch release = new CountDownLatch(1);

        private BlockingHandler() {
            this(1);
        }

        private BlockingHandler(int taskCount) {
            this.started = new CountDownLatch(taskCount);
        }

        @Override
        public String taskType() {
            return "resume.parse";
        }

        @Override
        public int schemaVersion() {
            return 1;
        }

        @Override
        public Class<String> payloadType() {
            return String.class;
        }

        @Override
        public void handle(AsyncTaskContext context, String payload) throws Exception {
            started.countDown();
            if (!release.await(5, TimeUnit.SECONDS)) {
                throw new IllegalStateException("等待测试释放处理器超时");
            }
        }
    }

    private static final class ProgressHandler implements AsyncTaskHandler<String> {
        @Override
        public String taskType() {
            return "resume.parse";
        }

        @Override
        public int schemaVersion() {
            return 1;
        }

        @Override
        public Class<String> payloadType() {
            return String.class;
        }

        @Override
        public void handle(AsyncTaskContext context, String payload) {
            context.updateProgress(new Progress(10, 20));
        }
    }

    private static final class LeaseLosingHandler implements AsyncTaskHandler<String> {
        private final JdbcTaskStore taskStore;

        private final AsyncTaskEnvelope envelope;

        private String recoveredLeaseToken;

        private LeaseLosingHandler(JdbcTaskStore taskStore, AsyncTaskEnvelope envelope) {
            this.taskStore = taskStore;
            this.envelope = envelope;
        }

        @Override
        public String taskType() {
            return "resume.parse";
        }

        @Override
        public int schemaVersion() {
            return 1;
        }

        @Override
        public Class<String> payloadType() {
            return String.class;
        }

        @Override
        public void handle(AsyncTaskContext context, String payload) {
            recoveredLeaseToken = taskStore.claimExecution(
                    envelope, NOW.plusSeconds(4), Duration.ofSeconds(10)).orElseThrow();
            throw new IllegalStateException("测试旧实例执行失败");
        }
    }

    private record Progress(int completed, int total) {
    }
}
