package com.zaviwayne.asynctask.jdbc;

import com.zaviwayne.asynctask.core.*;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.*;

/**
 * JDBC 异步任务状态存储。
 *
 * @since 2026-08-26
 */
public final class JdbcTaskStore {
    /**
     * 数据库错误信息最大长度。
     */
    private static final int MAX_ERROR_LENGTH = 2000;

    /**
     * 投递租约失效且重试次数耗尽时的错误信息。
     */
    private static final String EXHAUSTED_OUTBOX_ERROR = "投递实例失去租约且重试次数已耗尽";

    /**
     * 查询可投递 outbox 记录的 SQL。
     */
    private static final String SELECT_DISPATCHABLE_SQL = """
            SELECT task_id, envelope_json, status, dispatch_attempt
            FROM async_task_outbox
            WHERE (status = :pendingStatus AND dispatch_attempt < :maxAttempts)
               OR (status = :retryStatus
                   AND dispatch_attempt < :maxAttempts
                   AND next_attempt_at <= :now)
               OR (status = :uncertainStatus AND next_attempt_at <= :now)
               OR (status = :dispatchingStatus AND lease_until <= :now)
            ORDER BY created_at, task_id
            LIMIT :batchSize
            FOR UPDATE SKIP LOCKED
            """;

    /**
     * 将 outbox 记录更新为投递中的 SQL。
     */
    private static final String CLAIM_OUTBOX_SQL = """
            UPDATE async_task_outbox
            SET status = :dispatchingStatus,
                dispatch_attempt = dispatch_attempt + 1,
                lease_token = :leaseToken,
                lease_until = :leaseUntil,
                updated_at = :now
            WHERE task_id = :taskId
            """;

    /**
     * 锁定已耗尽重试次数的 outbox 记录的 SQL。
     */
    private static final String SELECT_EXHAUSTED_OUTBOX_SQL = """
            SELECT task_id, envelope_json, status, dispatch_attempt
            FROM async_task_outbox
            WHERE dispatch_attempt >= :maxAttempts
              AND status = :retryStatus
              AND next_attempt_at <= :now
            ORDER BY updated_at, task_id
            LIMIT :batchSize
            FOR UPDATE SKIP LOCKED
            """;

    /**
     * 将已耗尽重试次数的 outbox 记录置为死信的 SQL。
     */
    private static final String EXPIRE_EXHAUSTED_OUTBOX_SQL = """
            UPDATE async_task_outbox
            SET status = :deadStatus,
                lease_token = NULL,
                lease_until = NULL,
                last_error = :error,
                completed_at = :now,
                updated_at = :now
            WHERE task_id = :taskId
              AND dispatch_attempt >= :maxAttempts
              AND status = :retryStatus
              AND next_attempt_at <= :now
            """;

    /**
     * 查询幂等 outbox 记录的 SQL。
     */
    private static final String SELECT_OUTBOX_BY_IDEMPOTENCY_KEY_SQL = """
            SELECT task_id, envelope_json
            FROM async_task_outbox
            WHERE idempotency_key = :idempotencyKey
            """;

    /**
     * 完成 outbox 投递的 SQL。
     */
    private static final String COMPLETE_OUTBOX_SQL = """
            UPDATE async_task_outbox
            SET status = :dispatchedStatus,
                lease_token = NULL,
                lease_until = NULL,
                next_attempt_at = NULL,
                last_error = NULL,
                dispatched_at = :now,
                updated_at = :now
            WHERE task_id = :taskId
              AND status = :dispatchingStatus
              AND lease_token = :leaseToken
            """;

    /**
     * 记录 outbox 投递失败的 SQL。
     */
    private static final String FAIL_OUTBOX_SQL = """
            UPDATE async_task_outbox
            SET status = :targetStatus,
                lease_token = NULL,
                lease_until = NULL,
                next_attempt_at = :nextAttemptAt,
                last_error = :error,
                completed_at = CASE WHEN :targetStatus = :deadStatus THEN :now ELSE completed_at END,
                updated_at = :now
            WHERE task_id = :taskId
              AND status = :dispatchingStatus
              AND lease_token = :leaseToken
            """;

    /**
     * 锁定待执行 outbox 记录的 SQL。
     */
    private static final String SELECT_EXECUTION_FOR_UPDATE_SQL = """
            SELECT envelope_json, status, lease_until
            FROM async_task_outbox
            WHERE task_id = :taskId OR idempotency_key = :idempotencyKey
            FOR UPDATE
            """;

    /**
     * 按任务 ID 锁定死信对应 outbox 记录的 SQL。
     */
    private static final String SELECT_DEAD_LETTER_FOR_UPDATE_SQL = """
            SELECT envelope_json, status, lease_until
            FROM async_task_outbox
            WHERE task_id = :taskId
            FOR UPDATE
            """;

    /**
     * 抢占 outbox 执行租约的 SQL。
     */
    private static final String CLAIM_EXECUTION_SQL = """
            UPDATE async_task_outbox
            SET status = :runningStatus,
                execution_attempt = execution_attempt + 1,
                lease_token = :leaseToken,
                lease_until = :leaseUntil,
                next_attempt_at = NULL,
                last_error = NULL,
                dispatched_at = COALESCE(dispatched_at, :now),
                updated_at = :now
            WHERE task_id = :taskId
            """;

    /**
     * 刷新 outbox 执行租约的 SQL。
     */
    private static final String HEARTBEAT_EXECUTION_SQL = """
                UPDATE async_task_outbox
                SET lease_until = :leaseUntil,
                    updated_at = :now
                WHERE task_id = :taskId
                  AND status = :runningStatus
                  AND lease_token = :leaseToken
            """;

    /**
     * 更新任务进度并刷新 outbox 执行租约的 SQL。
     */
    private static final String UPDATE_EXECUTION_PROGRESS_SQL = """
            UPDATE async_task_outbox
            SET progress_json = :progressJson,
                lease_until = :leaseUntil,
                updated_at = :now
            WHERE task_id = :taskId
              AND status = :runningStatus
              AND lease_token = :leaseToken
            """;

    /**
     * 完成 outbox 任务执行的 SQL。
     */
    private static final String COMPLETE_EXECUTION_SQL = """
                UPDATE async_task_outbox
                SET status = :successStatus,
                    lease_token = NULL,
                    lease_until = NULL,
                    last_error = NULL,
                    completed_at = :now,
                    updated_at = :now
                WHERE task_id = :taskId
                  AND status = :runningStatus
                  AND lease_token = :leaseToken
            """;

    /**
     * 记录 outbox 任务执行失败的 SQL。
     */
    private static final String FAIL_EXECUTION_SQL = """
                UPDATE async_task_outbox
                SET status = :failedStatus,
                    lease_token = NULL,
                    lease_until = NULL,
                    last_error = :error,
                    updated_at = :now
                WHERE task_id = :taskId
                  AND status = :runningStatus
                  AND lease_token = :leaseToken
            """;

    /**
     * 将 outbox 记录置为死信的 SQL。
     */
    private static final String DEAD_OUTBOX_SQL = """
            UPDATE async_task_outbox
            SET status = :deadStatus,
                lease_token = NULL,
                lease_until = NULL,
                last_error = :error,
                completed_at = :now,
                updated_at = :now
            WHERE task_id = :taskId
              AND status = :failedStatus
            """;

    /**
     * JDBC 操作模板。
     */
    private final NamedParameterJdbcTemplate jdbcTemplate;

    /**
     * 短事务模板。
     */
    private final TransactionTemplate transactionTemplate;

    /**
     * 数据库方言。
     */
    private final AsyncTaskJdbcDialect dialect;

    /**
     * 消息序列化器。
     */
    private final TaskPayloadSerializer serializer;

    /**
     * 任务内容大小限制。
     */
    private final AsyncTaskContentLimits contentLimits;

    /**
     * 任务生命周期观测器。
     */
    private final AsyncTaskObserver observer;

    /**
     * 任务查询与管理存储。
     */
    private final JdbcTaskAdminStore adminStore;

    /**
     * 创建 JDBC 异步任务状态存储。
     *
     * @param jdbcTemplate       JDBC 操作模板
     * @param transactionManager 事务管理器
     * @param dialect            数据库方言
     * @param serializer         消息序列化器
     */
    public JdbcTaskStore(NamedParameterJdbcTemplate jdbcTemplate,
                         PlatformTransactionManager transactionManager,
                         AsyncTaskJdbcDialect dialect,
                         TaskPayloadSerializer serializer) {
        this(jdbcTemplate, transactionManager, dialect, serializer,
                NoOpAsyncTaskObserver.INSTANCE, AsyncTaskContentLimits.defaults());
    }

    /**
     * 创建带生命周期观测能力的 JDBC 异步任务状态存储。
     *
     * @param jdbcTemplate       JDBC 操作模板
     * @param transactionManager 事务管理器
     * @param dialect            数据库方言
     * @param serializer         消息序列化器
     * @param observer           任务生命周期观测器
     */
    public JdbcTaskStore(NamedParameterJdbcTemplate jdbcTemplate,
                         PlatformTransactionManager transactionManager,
                         AsyncTaskJdbcDialect dialect,
                         TaskPayloadSerializer serializer,
                         AsyncTaskObserver observer) {
        this(jdbcTemplate, transactionManager, dialect, serializer,
                observer, AsyncTaskContentLimits.defaults());
    }

    /**
     * 创建带生命周期观测和内容限制的 JDBC 异步任务状态存储。
     *
     * @param jdbcTemplate       JDBC 操作模板
     * @param transactionManager 事务管理器
     * @param dialect            数据库方言
     * @param serializer         消息序列化器
     * @param observer           任务生命周期观测器
     * @param contentLimits      任务内容大小限制
     */
    public JdbcTaskStore(NamedParameterJdbcTemplate jdbcTemplate,
                         PlatformTransactionManager transactionManager,
                         AsyncTaskJdbcDialect dialect,
                         TaskPayloadSerializer serializer,
                         AsyncTaskObserver observer,
                         AsyncTaskContentLimits contentLimits) {
        this.jdbcTemplate = Objects.requireNonNull(jdbcTemplate, "JDBC 操作模板不能为空");
        this.transactionTemplate = new TransactionTemplate(
                Objects.requireNonNull(transactionManager, "事务管理器不能为空"));
        this.transactionTemplate.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);
        this.dialect = Objects.requireNonNull(dialect, "数据库方言不能为空");
        this.serializer = Objects.requireNonNull(serializer, "消息序列化器不能为空");
        this.contentLimits = Objects.requireNonNull(contentLimits, "任务内容大小限制不能为空");
        this.observer = FailureIsolatingAsyncTaskObserver.wrap(observer);
        this.adminStore = new JdbcTaskAdminStore(
                this.jdbcTemplate, this.transactionTemplate, this.dialect,
                this.serializer, this.contentLimits, this.observer);
    }

    /**
     * 判断当前线程是否持有此存储所用数据源的活动事务资源。
     *
     * @return 存在数据库事务资源时返回 true
     */
    public boolean isDatabaseTransactionActive() {
        javax.sql.DataSource dataSource = jdbcTemplate.getJdbcTemplate().getDataSource();
        return dataSource != null
                && TransactionSynchronizationManager.isActualTransactionActive()
                && TransactionSynchronizationManager.hasResource(dataSource);
    }

    /**
     * 在调用方当前事务内持久化 outbox 消息。
     *
     * @param envelope 消息信封
     * @return 新增或已存在记录的任务 ID
     * @throws DuplicateAsyncTaskException 幂等键对应不同任务内容时抛出
     */
    public UUID saveOutbox(AsyncTaskEnvelope envelope) {
        Objects.requireNonNull(envelope, "消息信封不能为空");
        if (!isDatabaseTransactionActive()) {
            throw new IllegalStateException("Outbox 消息必须在活动的数据库事务中保存");
        }
        String envelopeJson = serializer.serialize(envelope);
        contentLimits.validateEnvelopeJson(envelopeJson);
        Instant now = envelope.createdAt();
        MapSqlParameterSource parameters = envelopeParameters(envelope)
                .addValue("envelopeJson", envelopeJson)
                .addValue("status", OutboxStatus.PENDING.getCode())
                .addValue("now", timestamp(now));
        int inserted;
        try {
            inserted = jdbcTemplate.update(dialect.insertOutboxSql(), parameters);
        } catch (DuplicateKeyException exception) {
            inserted = 0;
        }
        List<OutboxIdentity> identities = jdbcTemplate.query(
                SELECT_OUTBOX_BY_IDEMPOTENCY_KEY_SQL,
                new MapSqlParameterSource("idempotencyKey", envelope.idempotencyKey()),
                this::mapOutboxIdentity);
        if (identities.size() != 1) {
            throw new IllegalStateException("无法读取刚写入的 outbox 记录");
        }
        OutboxIdentity identity = identities.getFirst();
        validateOutboxIdentity(identity, envelope);
        boolean newlyInserted = inserted == 1 && identity.taskId().equals(envelope.taskId());
        if (newlyInserted) {
            observeEnqueuedAfterCommit(envelope);
        }
        return identity.taskId();
    }

    /**
     * 抢占一批可投递 outbox 记录。
     *
     * @param batchSize     最大批量
     * @param maxAttempts   最大投递次数
     * @param now           当前时间
     * @param leaseDuration 租约时长
     * @param leaseToken    本批任务共用的租约令牌
     * @return 已获取租约的消息及其当前投递次数
     */
    public Map<AsyncTaskEnvelope, Integer> claimOutbox(int batchSize,
                                                       int maxAttempts,
                                                       Instant now,
                                                       Duration leaseDuration,
                                                       String leaseToken) {
        List<OutboxClaim> claims = claimOutboxClaims(
                batchSize, maxAttempts, now, leaseDuration, leaseToken);
        Map<AsyncTaskEnvelope, Integer> claimedEnvelopes = new LinkedHashMap<>(claims.size());
        for (OutboxClaim claim : claims) {
            claimedEnvelopes.put(claim.envelope(), claim.attempt());
        }
        return Collections.unmodifiableMap(claimedEnvelopes);
    }

    List<OutboxClaim> claimOutboxClaims(int batchSize,
                                        int maxAttempts,
                                        Instant now,
                                        Duration leaseDuration,
                                        String leaseToken) {
        if (batchSize <= 0) {
            throw new IllegalArgumentException("批量大小必须大于 0");
        }
        if (maxAttempts <= 0) {
            throw new IllegalArgumentException("最大投递次数必须大于 0");
        }
        Objects.requireNonNull(now, "当前时间不能为空");
        requirePositive(leaseDuration, "投递租约时长");
        if (leaseToken == null || leaseToken.isBlank()) {
            throw new IllegalArgumentException("租约令牌不能为空");
        }
        List<OutboxClaim> claims = transactionTemplate.execute(transactionStatus -> {
            expireExhaustedOutbox(maxAttempts, now, batchSize);
            MapSqlParameterSource queryParameters = new MapSqlParameterSource()
                    .addValue("maxAttempts", maxAttempts)
                    .addValue("pendingStatus", OutboxStatus.PENDING.getCode())
                    .addValue("retryStatus", OutboxStatus.RETRY.getCode())
                    .addValue("uncertainStatus", OutboxStatus.DELIVERY_UNCERTAIN.getCode())
                    .addValue("dispatchingStatus", OutboxStatus.DISPATCHING.getCode())
                    .addValue("now", timestamp(now))
                    .addValue("batchSize", batchSize);
            List<OutboxRow> rows = jdbcTemplate.query(
                    SELECT_DISPATCHABLE_SQL, queryParameters, this::mapOutboxRow);
            List<OutboxClaim> claimedEnvelopes = new ArrayList<>(rows.size());
            for (OutboxRow row : rows) {
                boolean deliveryUncertain = claimOutboxRow(row, now, leaseDuration, leaseToken);
                claimedEnvelopes.add(new OutboxClaim(
                        row.envelope(), row.dispatchAttempt() + 1, deliveryUncertain));
            }
            return List.copyOf(claimedEnvelopes);
        });
        return Objects.requireNonNull(claims, "抢占 outbox 事务未返回结果");
    }

    /**
     * 将 outbox 记录标记为投递成功。
     *
     * @param taskId     任务 ID
     * @param leaseToken 租约令牌
     * @param now        完成时间
     * @return 状态更新成功时返回 true
     */
    public boolean markOutboxDispatched(UUID taskId, String leaseToken, Instant now) {
        MapSqlParameterSource parameters = leaseParameters(taskId, leaseToken, now)
                .addValue("dispatchedStatus", OutboxStatus.DISPATCHED.getCode())
                .addValue("dispatchingStatus", OutboxStatus.DISPATCHING.getCode());
        return executeUpdate(COMPLETE_OUTBOX_SQL, parameters);
    }

    /**
     * 记录 outbox 投递失败并安排重试或死信。
     *
     * @param taskId        任务 ID
     * @param leaseToken    租约令牌
     * @param error         错误信息
     * @param nextAttemptAt 下次执行时间，传入 null 表示进入死信
     * @param now           当前时间
     * @return 状态更新成功时返回 true
     */
    public boolean markOutboxFailed(UUID taskId,
                                    String leaseToken,
                                    String error,
                                    Instant nextAttemptAt,
                                    Instant now) {
        int targetStatus = nextAttemptAt == null
                ? OutboxStatus.DEAD.getCode()
                : OutboxStatus.RETRY.getCode();
        MapSqlParameterSource parameters = leaseParameters(taskId, leaseToken, now)
                .addValue("targetStatus", targetStatus)
                .addValue("deadStatus", OutboxStatus.DEAD.getCode())
                .addValue("dispatchingStatus", OutboxStatus.DISPATCHING.getCode())
                .addValue("nextAttemptAt", timestampOrNull(nextAttemptAt))
                .addValue("error", truncateError(error));
        return executeUpdate(FAIL_OUTBOX_SQL, parameters);
    }

    /**
     * 记录 outbox 投递结果未知并安排再次投递。
     *
     * @param taskId        任务 ID
     * @param leaseToken    租约令牌
     * @param error         错误信息
     * @param nextAttemptAt 下次执行时间
     * @param now           当前时间
     * @return 状态更新成功时返回 true
     */
    public boolean markOutboxDeliveryUncertain(UUID taskId,
                                               String leaseToken,
                                               String error,
                                               Instant nextAttemptAt,
                                               Instant now) {
        MapSqlParameterSource parameters = leaseParameters(taskId, leaseToken, now)
                .addValue("targetStatus", OutboxStatus.DELIVERY_UNCERTAIN.getCode())
                .addValue("deadStatus", OutboxStatus.DEAD.getCode())
                .addValue("dispatchingStatus", OutboxStatus.DISPATCHING.getCode())
                .addValue("nextAttemptAt", timestamp(Objects.requireNonNull(
                        nextAttemptAt, "下次投递时间不能为空")))
                .addValue("error", truncateError(error));
        return executeUpdate(FAIL_OUTBOX_SQL, parameters);
    }

    /**
     * 抢占消息对应的 outbox 执行租约。
     *
     * @param envelope      消息信封
     * @param now           当前时间
     * @param leaseDuration 租约时长
     * @return 成功抢占时返回租约令牌，任务已完成或已死信时返回空值
     * @throws DuplicateAsyncTaskException      幂等键对应不同任务内容时抛出
     * @throws TaskExecutionInProgressException 任务正在其他实例执行时抛出
     */
    public Optional<String> claimExecution(AsyncTaskEnvelope envelope,
                                           Instant now,
                                           Duration leaseDuration) {
        Objects.requireNonNull(envelope, "消息信封不能为空");
        Objects.requireNonNull(now, "当前时间不能为空");
        requirePositive(leaseDuration, "执行租约时长");
        Optional<String> leaseToken = transactionTemplate.execute(transactionStatus -> {
            MapSqlParameterSource queryParameters = new MapSqlParameterSource()
                    .addValue("taskId", dialect.uuidParameter(envelope.taskId().toString()))
                    .addValue("idempotencyKey", envelope.idempotencyKey());
            List<ExecutionRow> rows = jdbcTemplate.query(
                    SELECT_EXECUTION_FOR_UPDATE_SQL, queryParameters, this::mapExecutionRow);
            if (rows.isEmpty()) {
                throw new IllegalStateException("消息对应的 outbox 记录不存在: " + envelope.taskId());
            }
            if (rows.size() != 1) {
                throw new DuplicateAsyncTaskException("Outbox 幂等键与其他任务发生冲突");
            }
            ExecutionRow row = rows.getFirst();
            if (isStaleGeneration(row, envelope)) {
                return Optional.empty();
            }
            validateExecutionIdentity(row, envelope);
            return resolveExecutionLease(row, now, leaseDuration);
        });
        return Objects.requireNonNull(leaseToken, "抢占 outbox 执行事务未返回结果");
    }

    /**
     * 刷新 outbox 执行租约。
     *
     * @param taskId        任务 ID
     * @param leaseToken    租约令牌
     * @param now           当前时间
     * @param leaseDuration 租约时长
     * @return 刷新成功时返回 true
     */
    public boolean heartbeatExecution(UUID taskId,
                                      String leaseToken,
                                      Instant now,
                                      Duration leaseDuration) {
        requirePositive(leaseDuration, "执行租约时长");
        MapSqlParameterSource parameters = leaseParameters(taskId, leaseToken, now)
                .addValue("leaseUntil", timestamp(now.plus(leaseDuration)))
                .addValue("runningStatus", OutboxStatus.RUNNING.getCode());
        return executeUpdate(HEARTBEAT_EXECUTION_SQL, parameters);
    }

    /**
     * 持久化任务进度并刷新 outbox 执行租约。
     *
     * @param taskId        任务 ID
     * @param leaseToken    租约令牌
     * @param progressJson  进度 JSON
     * @param now           当前时间
     * @param leaseDuration 租约时长
     * @return 更新成功时返回 true
     */
    public boolean updateExecutionProgress(UUID taskId,
                                           String leaseToken,
                                           String progressJson,
                                           Instant now,
                                           Duration leaseDuration) {
        contentLimits.validateProgressJson(progressJson);
        requirePositive(leaseDuration, "执行租约时长");
        MapSqlParameterSource parameters = leaseParameters(taskId, leaseToken, now)
                .addValue("progressJson", progressJson)
                .addValue("leaseUntil", timestamp(now.plus(leaseDuration)))
                .addValue("runningStatus", OutboxStatus.RUNNING.getCode());
        return executeUpdate(UPDATE_EXECUTION_PROGRESS_SQL, parameters);
    }

    /**
     * 将 outbox 记录标记为执行成功。
     *
     * @param taskId     任务 ID
     * @param leaseToken 租约令牌
     * @param now        完成时间
     * @return 状态更新成功时返回 true
     */
    public boolean markExecutionCompleted(UUID taskId, String leaseToken, Instant now) {
        MapSqlParameterSource parameters = leaseParameters(taskId, leaseToken, now)
                .addValue("successStatus", OutboxStatus.SUCCESS.getCode())
                .addValue("runningStatus", OutboxStatus.RUNNING.getCode());
        return executeUpdate(COMPLETE_EXECUTION_SQL, parameters);
    }

    /**
     * 将 outbox 记录标记为本次执行失败。
     *
     * @param taskId     任务 ID
     * @param leaseToken 租约令牌
     * @param error      错误信息
     * @param now        当前时间
     * @return 状态更新成功时返回 true
     */
    public boolean markExecutionFailed(UUID taskId, String leaseToken, String error, Instant now) {
        MapSqlParameterSource parameters = leaseParameters(taskId, leaseToken, now)
                .addValue("failedStatus", OutboxStatus.FAILED.getCode())
                .addValue("runningStatus", OutboxStatus.RUNNING.getCode())
                .addValue("error", truncateError(error));
        return executeUpdate(FAIL_EXECUTION_SQL, parameters);
    }

    /**
     * 将 outbox 记录标记为死信终态。
     *
     * @param envelope 消息信封
     * @param error    死信原因
     * @param now      当前时间
     * @return 状态更新成功时返回 true
     * @throws DuplicateAsyncTaskException Outbox 记录与死信消息身份不一致时抛出
     */
    public boolean markOutboxDead(AsyncTaskEnvelope envelope, String error, Instant now) {
        Objects.requireNonNull(envelope, "消息信封不能为空");
        Objects.requireNonNull(now, "当前时间不能为空");
        Boolean markedDead = transactionTemplate.execute(transactionStatus -> {
            MapSqlParameterSource parameters = new MapSqlParameterSource()
                    .addValue("taskId", dialect.uuidParameter(envelope.taskId().toString()));
            List<ExecutionRow> rows = jdbcTemplate.query(
                    SELECT_DEAD_LETTER_FOR_UPDATE_SQL, parameters, this::mapExecutionRow);
            if (rows.isEmpty()) {
                return false;
            }
            ExecutionRow row = rows.getFirst();
            if (isStaleGeneration(row, envelope)) {
                return false;
            }
            validateExecutionIdentity(row, envelope);
            if (row.status() != OutboxStatus.FAILED.getCode()) {
                return false;
            }
            parameters.addValue("deadStatus", OutboxStatus.DEAD.getCode())
                    .addValue("failedStatus", OutboxStatus.FAILED.getCode())
                    .addValue("error", truncateError(error))
                    .addValue("now", timestamp(now));
            return jdbcTemplate.update(DEAD_OUTBOX_SQL, parameters) == 1;
        });
        return Objects.requireNonNull(markedDead, "标记 outbox 死信事务未返回结果");
    }

    /**
     * 按任务 ID 查询任务信息。
     *
     * @param taskId 任务 ID
     * @return 任务存在时返回任务信息
     */
    public Optional<AsyncTaskInfo> findByTaskId(UUID taskId) {
        return adminStore.findByTaskId(taskId);
    }

    /**
     * 按幂等键查询任务信息。
     *
     * @param idempotencyKey 幂等键
     * @return 任务存在时返回任务信息
     */
    public Optional<AsyncTaskInfo> findByIdempotencyKey(String idempotencyKey) {
        return adminStore.findByIdempotencyKey(idempotencyKey);
    }

    /**
     * 按业务关联查询任务信息。
     *
     * @param referenceType 业务关联类型
     * @param referenceId   业务关联标识
     * @param limit         最大返回数量
     * @param offset        分页偏移量
     * @return 符合条件的任务列表
     */
    public List<AsyncTaskInfo> findByReference(String referenceType,
                                               String referenceId,
                                               int limit,
                                               long offset) {
        return adminStore.findByReference(referenceType, referenceId, limit, offset);
    }

    /**
     * 检查异步任务表是否可以访问。
     */
    public void checkHealth() {
        adminStore.checkHealth();
    }

    /**
     * 查询异步任务运行统计。
     *
     * @return 异步任务运行统计
     */
    public AsyncTaskStatistics statistics() {
        return adminStore.statistics();
    }

    /**
     * 将终态任务重新加入投递队列。
     *
     * @param taskId 任务 ID
     * @param now    当前时间
     * @return 重新入队结果
     */
    public AsyncTaskRequeueResult requeue(UUID taskId, Instant now) {
        return adminStore.requeue(taskId, now);
    }

    /**
     * 分批删除早于截止时间的终态任务。
     *
     * @param cutoff    截止时间
     * @param batchSize 最大删除数量
     * @return 实际删除数量
     */
    public int deleteTerminalTasks(Instant cutoff, int batchSize) {
        return adminStore.deleteTerminalTasks(cutoff, batchSize);
    }

    private void expireExhaustedOutbox(int maxAttempts, Instant now, int batchSize) {
        MapSqlParameterSource parameters = new MapSqlParameterSource()
                .addValue("maxAttempts", maxAttempts)
                .addValue("deadStatus", OutboxStatus.DEAD.getCode())
                .addValue("retryStatus", OutboxStatus.RETRY.getCode())
                .addValue("dispatchingStatus", OutboxStatus.DISPATCHING.getCode())
                .addValue("error", EXHAUSTED_OUTBOX_ERROR)
                .addValue("now", timestamp(now))
                .addValue("batchSize", batchSize);
        List<OutboxRow> exhaustedRows = jdbcTemplate.query(
                SELECT_EXHAUSTED_OUTBOX_SQL, parameters, this::mapOutboxRow);
        for (OutboxRow row : exhaustedRows) {
            parameters.addValue("taskId", dialect.uuidParameter(row.envelope().taskId().toString()));
            int updated = jdbcTemplate.update(EXPIRE_EXHAUSTED_OUTBOX_SQL, parameters);
            if (updated != 1) {
                throw new IllegalStateException("无法将已耗尽投递次数的 outbox 记录置为死信");
            }
            observeDispatchFailureAfterCommit(row.envelope());
        }
    }

    private boolean claimOutboxRow(OutboxRow row,
                                   Instant now,
                                   Duration leaseDuration,
                                   String leaseToken) {
        boolean recovered = row.status() == OutboxStatus.DISPATCHING.getCode();
        boolean deliveryUncertain = recovered
                || row.status() == OutboxStatus.DELIVERY_UNCERTAIN.getCode();
        MapSqlParameterSource parameters = new MapSqlParameterSource()
                .addValue("taskId", dialect.uuidParameter(row.envelope().taskId().toString()))
                .addValue("dispatchingStatus", OutboxStatus.DISPATCHING.getCode())
                .addValue("leaseToken", dialect.uuidParameter(leaseToken))
                .addValue("leaseUntil", timestamp(now.plus(leaseDuration)))
                .addValue("now", timestamp(now));
        int updated = jdbcTemplate.update(CLAIM_OUTBOX_SQL, parameters);
        if (updated != 1) {
            throw new IllegalStateException("无法更新已锁定的 outbox 记录");
        }
        if (recovered) {
            observeDispatchLeaseRecoveredAfterCommit(row.envelope());
        }
        return deliveryUncertain;
    }

    private Optional<String> resolveExecutionLease(ExecutionRow row,
                                                   Instant now,
                                                   Duration leaseDuration) {
        if (row.status() == OutboxStatus.SUCCESS.getCode()) {
            return Optional.empty();
        }
        if (row.status() == OutboxStatus.DEAD.getCode()) {
            return Optional.empty();
        }
        boolean activeLease = row.status() == OutboxStatus.RUNNING.getCode()
                && row.leaseUntil() != null
                && row.leaseUntil().isAfter(now);
        if (activeLease) {
            throw new TaskExecutionInProgressException(
                    "异步任务正在其他实例执行: " + row.envelope().taskId());
        }
        boolean recovered = row.status() == OutboxStatus.RUNNING.getCode();
        String leaseToken = UUID.randomUUID().toString();
        MapSqlParameterSource parameters = new MapSqlParameterSource()
                .addValue("taskId", dialect.uuidParameter(row.envelope().taskId().toString()))
                .addValue("runningStatus", OutboxStatus.RUNNING.getCode())
                .addValue("leaseToken", dialect.uuidParameter(leaseToken))
                .addValue("leaseUntil", timestamp(now.plus(leaseDuration)))
                .addValue("now", timestamp(now));
        int updated = jdbcTemplate.update(CLAIM_EXECUTION_SQL, parameters);
        if (updated != 1) {
            throw new IllegalStateException("无法更新已锁定的 outbox 执行记录");
        }
        if (recovered) {
            observeExecutionLeaseRecoveredAfterCommit(row.envelope());
        }
        return Optional.of(leaseToken);
    }

    private OutboxRow mapOutboxRow(ResultSet resultSet, int rowNumber) throws SQLException {
        AsyncTaskEnvelope envelope = serializer.deserialize(
                resultSet.getString("envelope_json"), AsyncTaskEnvelope.class);
        return new OutboxRow(
                envelope,
                resultSet.getInt("status"),
                resultSet.getInt("dispatch_attempt"));
    }

    private OutboxIdentity mapOutboxIdentity(ResultSet resultSet, int rowNumber) throws SQLException {
        return new OutboxIdentity(
                UUID.fromString(resultSet.getString("task_id")),
                serializer.deserialize(resultSet.getString("envelope_json"), AsyncTaskEnvelope.class));
    }

    private ExecutionRow mapExecutionRow(ResultSet resultSet, int rowNumber) throws SQLException {
        Timestamp leaseUntil = resultSet.getTimestamp("lease_until");
        return new ExecutionRow(
                serializer.deserialize(resultSet.getString("envelope_json"), AsyncTaskEnvelope.class),
                resultSet.getInt("status"),
                leaseUntil == null ? null : leaseUntil.toInstant());
    }

    private static void validateOutboxIdentity(OutboxIdentity identity, AsyncTaskEnvelope envelope) {
        AsyncTaskEnvelope persistedEnvelope = identity.envelope();
        boolean storedTaskIdMatches = identity.taskId().equals(persistedEnvelope.taskId());
        boolean sameTask = storedTaskIdMatches
                && persistedEnvelope.destination().equals(envelope.destination())
                && persistedEnvelope.taskType().equals(envelope.taskType())
                && persistedEnvelope.schemaVersion() == envelope.schemaVersion()
                && persistedEnvelope.payloadJson().equals(envelope.payloadJson())
                && persistedEnvelope.payloadHash().equals(envelope.payloadHash())
                && persistedEnvelope.idempotencyKey().equals(envelope.idempotencyKey())
                && Objects.equals(persistedEnvelope.referenceType(), envelope.referenceType())
                && Objects.equals(persistedEnvelope.referenceId(), envelope.referenceId())
                && persistedEnvelope.headers().equals(envelope.headers())
                && persistedEnvelope.generation() == envelope.generation();
        if (!sameTask) {
            throw new DuplicateAsyncTaskException("幂等键已被不同的异步任务使用: " + envelope.idempotencyKey());
        }
    }

    private static boolean isStaleGeneration(ExecutionRow row, AsyncTaskEnvelope envelope) {
        return envelope.generation() < row.envelope().generation();
    }

    private static void validateExecutionIdentity(ExecutionRow row, AsyncTaskEnvelope envelope) {
        if (!row.envelope().equals(envelope)) {
            throw new DuplicateAsyncTaskException("Outbox 记录与消费消息内容不一致: "
                    + envelope.idempotencyKey());
        }
    }

    private MapSqlParameterSource envelopeParameters(AsyncTaskEnvelope envelope) {
        return new MapSqlParameterSource()
                .addValue("taskId", dialect.uuidParameter(envelope.taskId().toString()))
                .addValue("destination", envelope.destination())
                .addValue("taskType", envelope.taskType())
                .addValue("schemaVersion", envelope.schemaVersion())
                .addValue("idempotencyKey", envelope.idempotencyKey())
                .addValue("referenceType", envelope.referenceType())
                .addValue("referenceId", envelope.referenceId())
                .addValue("payloadHash", envelope.payloadHash())
                .addValue("generation", envelope.generation());
    }

    private void observeDispatchFailureAfterCommit(AsyncTaskEnvelope envelope) {
        observeAfterCommit(() -> observer.onDispatchFailed(
                envelope.destination(), envelope.taskType(), true));
    }

    private void observeDispatchLeaseRecoveredAfterCommit(AsyncTaskEnvelope envelope) {
        observeAfterCommit(() -> observer.onDispatchLeaseRecovered(
                envelope.destination(), envelope.taskType()));
    }

    private void observeExecutionLeaseRecoveredAfterCommit(AsyncTaskEnvelope envelope) {
        observeAfterCommit(() -> observer.onExecutionLeaseRecovered(
                envelope.destination(), envelope.taskType()));
    }

    private void observeEnqueuedAfterCommit(AsyncTaskEnvelope envelope) {
        observeAfterCommit(() -> observer.onEnqueued(envelope.destination(), envelope.taskType()));
    }

    private static void observeAfterCommit(Runnable callback) {
        TransactionSynchronizationManager.registerSynchronization(
                new AfterCommitTransactionSynchronization(callback));
    }

    private MapSqlParameterSource leaseParameters(UUID taskId, String leaseToken, Instant now) {
        return new MapSqlParameterSource()
                .addValue("taskId", dialect.uuidParameter(
                        Objects.requireNonNull(taskId, "任务 ID 不能为空").toString()))
                .addValue("leaseToken", dialect.uuidParameter(
                        Objects.requireNonNull(leaseToken, "租约令牌不能为空")))
                .addValue("now", timestamp(Objects.requireNonNull(now, "当前时间不能为空")));
    }

    private static Timestamp timestamp(Instant instant) {
        return Timestamp.from(instant);
    }

    private static Timestamp timestampOrNull(Instant instant) {
        return instant == null ? null : timestamp(instant);
    }

    private static String truncateError(String error) {
        if (error == null || error.isBlank()) {
            return "未知异常";
        }
        return error.length() <= MAX_ERROR_LENGTH ? error : error.substring(0, MAX_ERROR_LENGTH);
    }

    private static void requirePositive(Duration duration, String fieldName) {
        Objects.requireNonNull(duration, fieldName + "不能为空");
        if (duration.isZero() || duration.isNegative()) {
            throw new IllegalArgumentException(fieldName + "必须大于 0");
        }
    }

    private boolean executeUpdate(String sql, MapSqlParameterSource parameters) {
        Boolean updated = transactionTemplate.execute(transactionStatus ->
                jdbcTemplate.update(sql, parameters) == 1);
        return Boolean.TRUE.equals(updated);
    }

}
