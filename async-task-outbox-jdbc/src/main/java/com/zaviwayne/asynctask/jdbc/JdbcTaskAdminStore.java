package com.zaviwayne.asynctask.jdbc;

import com.zaviwayne.asynctask.core.*;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

/**
 * JDBC 异步任务查询与管理存储。
 *
 * @since 2026-08-31
 */
final class JdbcTaskAdminStore {
    /**
     * 检查异步任务表可访问性的 SQL。
     */
    private static final String CHECK_HEALTH_SQL = "SELECT 1 FROM async_task_outbox WHERE 1 = 0";

    /**
     * 按任务 ID 查询任务信息的 SQL。
     */
    private static final String SELECT_TASK_BY_ID_SQL = """
            SELECT task_id, destination, task_type, schema_version, idempotency_key,
                   reference_type, reference_id, status, dispatch_attempt, execution_attempt,
                   next_attempt_at, last_error, progress_json, dispatched_at, completed_at,
                   created_at, updated_at
            FROM async_task_outbox
            WHERE task_id = :taskId
            """;

    /**
     * 按幂等键查询任务信息的 SQL。
     */
    private static final String SELECT_TASK_BY_IDEMPOTENCY_KEY_SQL = """
            SELECT task_id, destination, task_type, schema_version, idempotency_key,
                   reference_type, reference_id, status, dispatch_attempt, execution_attempt,
                   next_attempt_at, last_error, progress_json, dispatched_at, completed_at,
                   created_at, updated_at
            FROM async_task_outbox
            WHERE idempotency_key = :idempotencyKey
            """;

    /**
     * 按业务关联信息分页查询任务信息的 SQL。
     */
    private static final String SELECT_TASKS_BY_REFERENCE_SQL = """
            SELECT task_id, destination, task_type, schema_version, idempotency_key,
                   reference_type, reference_id, status, dispatch_attempt, execution_attempt,
                   next_attempt_at, last_error, progress_json, dispatched_at, completed_at,
                   created_at, updated_at
            FROM async_task_outbox
            WHERE reference_type = :referenceType
              AND reference_id = :referenceId
            ORDER BY created_at DESC, task_id DESC
            LIMIT :limit OFFSET :offset
            """;

    /**
     * 查询任务运行统计的 SQL。
     */
    private static final String SELECT_STATISTICS_SQL = """
            SELECT SUM(CASE WHEN status NOT IN (:deadStatus, :successStatus) THEN 1 ELSE 0 END) AS backlog_count,
                   SUM(CASE WHEN status = :runningStatus THEN 1 ELSE 0 END) AS running_count,
                   SUM(CASE WHEN status = :deadStatus THEN 1 ELSE 0 END) AS dead_count,
                   MIN(CASE WHEN status NOT IN (:deadStatus, :successStatus) THEN created_at END) AS oldest_backlog_at
            FROM async_task_outbox
            """;

    /**
     * 锁定待重新入队任务的 SQL。
     */
    private static final String SELECT_REQUEUE_TASK_SQL = """
            SELECT envelope_json, status
            FROM async_task_outbox
            WHERE task_id = :taskId
            FOR UPDATE
            """;

    /**
     * 将终态任务恢复为待投递状态的 SQL。
     */
    private static final String REQUEUE_TASK_SQL = """
            UPDATE async_task_outbox
            SET status = :pendingStatus,
                generation = :generation,
                envelope_json = :envelopeJson,
                dispatch_attempt = 0,
                execution_attempt = 0,
                next_attempt_at = NULL,
                lease_token = NULL,
                lease_until = NULL,
                last_error = NULL,
                progress_json = NULL,
                dispatched_at = NULL,
                completed_at = NULL,
                updated_at = :now
            WHERE task_id = :taskId
              AND status IN (:deadStatus, :successStatus)
            """;

    /**
     * 分批删除过期终态任务的 SQL。
     */
    private static final String DELETE_TERMINAL_TASKS_SQL = """
            DELETE FROM async_task_outbox
            WHERE task_id IN (
                SELECT task_id
                FROM (
                    SELECT task_id
                    FROM async_task_outbox
                    WHERE status IN (:deadStatus, :successStatus)
                      AND completed_at < :cutoff
                    ORDER BY completed_at, task_id
                    LIMIT :batchSize
                ) terminal_tasks
            )
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
     * 创建 JDBC 异步任务查询与管理存储。
     *
     * @param jdbcTemplate        JDBC 操作模板
     * @param transactionTemplate 短事务模板
     * @param dialect             数据库方言
     * @param serializer          消息序列化器
     * @param contentLimits       任务内容大小限制
     * @param observer            任务生命周期观测器
     */
    public JdbcTaskAdminStore(NamedParameterJdbcTemplate jdbcTemplate,
                              TransactionTemplate transactionTemplate,
                              AsyncTaskJdbcDialect dialect,
                              TaskPayloadSerializer serializer,
                              AsyncTaskContentLimits contentLimits,
                              AsyncTaskObserver observer) {
        this.jdbcTemplate = Objects.requireNonNull(jdbcTemplate, "JDBC 操作模板不能为空");
        this.transactionTemplate = Objects.requireNonNull(transactionTemplate, "短事务模板不能为空");
        this.dialect = Objects.requireNonNull(dialect, "数据库方言不能为空");
        this.serializer = Objects.requireNonNull(serializer, "消息序列化器不能为空");
        this.contentLimits = Objects.requireNonNull(contentLimits, "任务内容大小限制不能为空");
        this.observer = Objects.requireNonNull(observer, "任务生命周期观测器不能为空");
    }

    /**
     * 按任务 ID 查询任务信息。
     *
     * @param taskId 任务 ID
     * @return 任务存在时返回任务信息
     */
    public Optional<AsyncTaskInfo> findByTaskId(UUID taskId) {
        UUID requiredTaskId = Objects.requireNonNull(taskId, "任务 ID 不能为空");
        MapSqlParameterSource parameters = new MapSqlParameterSource()
                .addValue("taskId", dialect.uuidParameter(requiredTaskId.toString()));
        return jdbcTemplate.query(SELECT_TASK_BY_ID_SQL, parameters, JdbcTaskAdminStore::mapTaskInfo)
                .stream()
                .findFirst();
    }

    /**
     * 按幂等键查询任务信息。
     *
     * @param idempotencyKey 幂等键
     * @return 任务存在时返回任务信息
     */
    public Optional<AsyncTaskInfo> findByIdempotencyKey(String idempotencyKey) {
        MapSqlParameterSource parameters = new MapSqlParameterSource(
                "idempotencyKey", Objects.requireNonNull(idempotencyKey, "幂等键不能为空"));
        return jdbcTemplate.query(
                        SELECT_TASK_BY_IDEMPOTENCY_KEY_SQL, parameters, JdbcTaskAdminStore::mapTaskInfo)
                .stream()
                .findFirst();
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
        MapSqlParameterSource parameters = new MapSqlParameterSource()
                .addValue("referenceType", referenceType)
                .addValue("referenceId", referenceId)
                .addValue("limit", limit)
                .addValue("offset", offset);
        return jdbcTemplate.query(SELECT_TASKS_BY_REFERENCE_SQL, parameters, JdbcTaskAdminStore::mapTaskInfo);
    }

    /**
     * 检查异步任务表是否可以访问。
     */
    public void checkHealth() {
        jdbcTemplate.getJdbcOperations().execute(CHECK_HEALTH_SQL);
    }

    /**
     * 查询异步任务运行统计。
     *
     * @return 异步任务运行统计
     */
    public AsyncTaskStatistics statistics() {
        MapSqlParameterSource parameters = new MapSqlParameterSource()
                .addValue("deadStatus", OutboxStatus.DEAD.getCode())
                .addValue("successStatus", OutboxStatus.SUCCESS.getCode())
                .addValue("runningStatus", OutboxStatus.RUNNING.getCode());
        List<AsyncTaskStatistics> statistics = jdbcTemplate.query(
                SELECT_STATISTICS_SQL, parameters, JdbcTaskAdminStore::mapStatistics);
        if (statistics.size() != 1) {
            throw new IllegalStateException("无法读取异步任务运行统计");
        }
        return statistics.getFirst();
    }

    /**
     * 将终态任务重新加入投递队列。
     *
     * @param taskId 任务 ID
     * @param now    当前时间
     * @return 重新入队结果
     */
    public AsyncTaskRequeueResult requeue(UUID taskId, Instant now) {
        UUID requiredTaskId = Objects.requireNonNull(taskId, "任务 ID 不能为空");
        Instant requiredNow = Objects.requireNonNull(now, "当前时间不能为空");
        AsyncTaskRequeueResult result = transactionTemplate.execute(transactionStatus -> {
            MapSqlParameterSource parameters = new MapSqlParameterSource()
                    .addValue("taskId", dialect.uuidParameter(requiredTaskId.toString()));
            List<RequeueRow> rows = jdbcTemplate.query(
                    SELECT_REQUEUE_TASK_SQL, parameters, this::mapRequeueRow);
            if (rows.isEmpty()) {
                return AsyncTaskRequeueResult.NOT_FOUND;
            }
            RequeueRow row = rows.getFirst();
            boolean terminal = row.status() == OutboxStatus.DEAD.getCode()
                    || row.status() == OutboxStatus.SUCCESS.getCode();
            if (!terminal) {
                return AsyncTaskRequeueResult.NOT_TERMINAL;
            }
            AsyncTaskEnvelope nextEnvelope = row.envelope().nextGeneration();
            String envelopeJson = serializer.serialize(nextEnvelope);
            contentLimits.validateEnvelopeJson(envelopeJson);
            parameters.addValue("pendingStatus", OutboxStatus.PENDING.getCode())
                    .addValue("deadStatus", OutboxStatus.DEAD.getCode())
                    .addValue("successStatus", OutboxStatus.SUCCESS.getCode())
                    .addValue("generation", nextEnvelope.generation())
                    .addValue("envelopeJson", envelopeJson)
                    .addValue("now", timestamp(requiredNow));
            int updated = jdbcTemplate.update(REQUEUE_TASK_SQL, parameters);
            if (updated != 1) {
                throw new IllegalStateException("终态任务重新入队失败: " + requiredTaskId);
            }
            observeAfterCommit(() -> observer.onRequeued(
                    nextEnvelope.destination(), nextEnvelope.taskType()));
            return AsyncTaskRequeueResult.REQUEUED;
        });
        return Objects.requireNonNull(result, "重新入队事务未返回结果");
    }

    /**
     * 分批删除早于截止时间的终态任务。
     *
     * @param cutoff    截止时间
     * @param batchSize 最大删除数量
     * @return 实际删除数量
     */
    public int deleteTerminalTasks(Instant cutoff, int batchSize) {
        Instant requiredCutoff = Objects.requireNonNull(cutoff, "清理截止时间不能为空");
        if (batchSize <= 0) {
            throw new IllegalArgumentException("清理批量大小必须大于 0");
        }
        Integer deleted = transactionTemplate.execute(transactionStatus -> {
            MapSqlParameterSource parameters = new MapSqlParameterSource()
                    .addValue("deadStatus", OutboxStatus.DEAD.getCode())
                    .addValue("successStatus", OutboxStatus.SUCCESS.getCode())
                    .addValue("cutoff", timestamp(requiredCutoff))
                    .addValue("batchSize", batchSize);
            int deletedCount = jdbcTemplate.update(DELETE_TERMINAL_TASKS_SQL, parameters);
            if (deletedCount > 0) {
                observeAfterCommit(() -> observer.onCleaned(deletedCount));
            }
            return deletedCount;
        });
        return Objects.requireNonNull(deleted, "清理终态任务事务未返回结果");
    }

    private static AsyncTaskInfo mapTaskInfo(ResultSet resultSet, int rowNumber) throws SQLException {
        return new AsyncTaskInfo(
                UUID.fromString(resultSet.getString("task_id")),
                resultSet.getString("destination"),
                resultSet.getString("task_type"),
                resultSet.getInt("schema_version"),
                resultSet.getString("idempotency_key"),
                resultSet.getString("reference_type"),
                resultSet.getString("reference_id"),
                OutboxStatus.fromCode(resultSet.getInt("status")).toPublicStatus(),
                resultSet.getInt("dispatch_attempt"),
                resultSet.getInt("execution_attempt"),
                instantOrNull(resultSet, "next_attempt_at"),
                resultSet.getString("last_error"),
                resultSet.getString("progress_json"),
                instantOrNull(resultSet, "dispatched_at"),
                instantOrNull(resultSet, "completed_at"),
                instantOrNull(resultSet, "created_at"),
                instantOrNull(resultSet, "updated_at"));
    }

    private static AsyncTaskStatistics mapStatistics(ResultSet resultSet, int rowNumber) throws SQLException {
        return new AsyncTaskStatistics(
                resultSet.getLong("backlog_count"),
                resultSet.getLong("running_count"),
                resultSet.getLong("dead_count"),
                instantOrNull(resultSet, "oldest_backlog_at"));
    }

    private RequeueRow mapRequeueRow(ResultSet resultSet, int rowNumber) throws SQLException {
        AsyncTaskEnvelope envelope = serializer.deserialize(
                resultSet.getString("envelope_json"), AsyncTaskEnvelope.class);
        return new RequeueRow(envelope, resultSet.getInt("status"));
    }

    private static Instant instantOrNull(ResultSet resultSet, String columnName) throws SQLException {
        Timestamp timestamp = resultSet.getTimestamp(columnName);
        return timestamp == null ? null : timestamp.toInstant();
    }

    private static Timestamp timestamp(Instant instant) {
        return Timestamp.from(instant);
    }

    private static void observeAfterCommit(Runnable callback) {
        TransactionSynchronizationManager.registerSynchronization(
                new AfterCommitTransactionSynchronization(callback));
    }
}
