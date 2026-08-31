CREATE TABLE IF NOT EXISTS async_task_outbox (
    task_id UUID PRIMARY KEY,
    destination VARCHAR(255) NOT NULL,
    task_type VARCHAR(200) NOT NULL,
    schema_version INTEGER NOT NULL,
    idempotency_key VARCHAR(255) NOT NULL,
    reference_type VARCHAR(100),
    reference_id VARCHAR(255),
    payload_hash VARCHAR(128) NOT NULL,
    envelope_json TEXT NOT NULL,
    status SMALLINT NOT NULL DEFAULT 0,
    dispatch_attempt INTEGER NOT NULL DEFAULT 0,
    execution_attempt INTEGER NOT NULL DEFAULT 0,
    generation INTEGER NOT NULL DEFAULT 0,
    next_attempt_at TIMESTAMPTZ,
    lease_token UUID,
    lease_until TIMESTAMPTZ,
    last_error VARCHAR(2000),
    progress_json TEXT,
    dispatched_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uk_async_task_outbox_idempotency UNIQUE (idempotency_key),
    CONSTRAINT ck_async_task_outbox_status CHECK (status IN (0, 1, 2, 3, 4, 5, 6, 7, 8)),
    CONSTRAINT ck_async_task_outbox_schema_version CHECK (schema_version > 0),
    CONSTRAINT ck_async_task_outbox_dispatch_attempt CHECK (dispatch_attempt >= 0),
    CONSTRAINT ck_async_task_outbox_execution_attempt CHECK (execution_attempt >= 0),
    CONSTRAINT ck_async_task_outbox_generation CHECK (generation >= 0),
    CONSTRAINT ck_async_task_outbox_reference CHECK (
        (reference_type IS NULL AND reference_id IS NULL)
        OR (reference_type IS NOT NULL AND reference_id IS NOT NULL)
    ),
    CONSTRAINT ck_async_task_outbox_lease CHECK (
        (status IN (1, 5) AND lease_token IS NOT NULL AND lease_until IS NOT NULL)
        OR (status NOT IN (1, 5) AND lease_token IS NULL AND lease_until IS NULL)
    ),
    CONSTRAINT ck_async_task_outbox_retry_schedule CHECK (
        (status IN (3, 8) AND next_attempt_at IS NOT NULL)
        OR status NOT IN (3, 8)
    )
);

COMMENT ON TABLE async_task_outbox IS '异步任务发件箱，记录任务投递与消费执行的完整生命周期';
COMMENT ON COLUMN async_task_outbox.task_id IS '任务唯一标识';
COMMENT ON COLUMN async_task_outbox.destination IS '消息投递目标，例如 Kafka 主题';
COMMENT ON COLUMN async_task_outbox.task_type IS '任务类型，用于匹配消费处理器';
COMMENT ON COLUMN async_task_outbox.schema_version IS '消息负载结构版本，必须大于 0';
COMMENT ON COLUMN async_task_outbox.idempotency_key IS '业务幂等键，全表唯一';
COMMENT ON COLUMN async_task_outbox.reference_type IS '可选的业务关联类型，用于跨任务检索';
COMMENT ON COLUMN async_task_outbox.reference_id IS '可选的业务关联标识，与业务关联类型成对出现';
COMMENT ON COLUMN async_task_outbox.payload_hash IS 'JSON 负载的 SHA-256 摘要，用于快速校验载荷一致性';
COMMENT ON COLUMN async_task_outbox.envelope_json IS '完整的版本化消息信封 JSON';
COMMENT ON COLUMN async_task_outbox.status IS '任务状态：0-等待投递，1-投递中，2-已投递，3-等待投递重试，4-死信终态，5-执行中，6-执行成功，7-执行失败等待消息重投，8-投递结果未知等待重试';
COMMENT ON COLUMN async_task_outbox.dispatch_attempt IS '已抢占的投递次数，首次投递抢占时记为 1';
COMMENT ON COLUMN async_task_outbox.execution_attempt IS '已抢占的执行次数，首次执行抢占时记为 1';
COMMENT ON COLUMN async_task_outbox.generation IS '重新入队代际，初始为 0，每次重新入队递增';
COMMENT ON COLUMN async_task_outbox.next_attempt_at IS '下一次允许投递的 UTC 时刻，仅等待投递重试状态必填';
COMMENT ON COLUMN async_task_outbox.lease_token IS '当前投递或执行租约令牌，用于防止过期执行者回写状态';
COMMENT ON COLUMN async_task_outbox.lease_until IS '当前投递或执行租约到期的 UTC 时刻，过期后可被其他实例接管';
COMMENT ON COLUMN async_task_outbox.last_error IS '最近一次投递或执行失败的错误摘要，最长 2000 字符';
COMMENT ON COLUMN async_task_outbox.progress_json IS '业务处理器最近一次上报的任务进度 JSON';
COMMENT ON COLUMN async_task_outbox.dispatched_at IS '消息已投递或首次被消费端接收的 UTC 时刻';
COMMENT ON COLUMN async_task_outbox.completed_at IS '任务执行成功或进入死信终态的 UTC 时刻';
COMMENT ON COLUMN async_task_outbox.created_at IS '记录创建的 UTC 时刻';
COMMENT ON COLUMN async_task_outbox.updated_at IS '记录最后更新的 UTC 时刻';

CREATE INDEX IF NOT EXISTS idx_async_task_outbox_pending_claim
    ON async_task_outbox (created_at, task_id)
    WHERE status = 0;

CREATE INDEX IF NOT EXISTS idx_async_task_outbox_retry_claim
    ON async_task_outbox (next_attempt_at, created_at, task_id)
    WHERE status IN (3, 8);

CREATE INDEX IF NOT EXISTS idx_async_task_outbox_lease_claim
    ON async_task_outbox (lease_until, created_at, task_id)
    WHERE status IN (1, 5);

CREATE INDEX IF NOT EXISTS idx_async_task_outbox_reference
    ON async_task_outbox (reference_type, reference_id, created_at DESC, task_id DESC);

CREATE INDEX IF NOT EXISTS idx_async_task_outbox_cleanup
    ON async_task_outbox (completed_at, task_id) INCLUDE (status)
    WHERE completed_at IS NOT NULL;
