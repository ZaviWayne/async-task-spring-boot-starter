CREATE TABLE IF NOT EXISTS async_task_outbox (
    task_id CHAR(36) CHARACTER SET ascii COLLATE ascii_bin NOT NULL COMMENT '任务唯一标识',
    destination VARCHAR(255) NOT NULL COMMENT '消息投递目标，例如 Kafka 主题',
    task_type VARCHAR(200) NOT NULL COMMENT '任务类型，用于匹配消费处理器',
    schema_version INTEGER NOT NULL COMMENT '消息负载结构版本，必须大于 0',
    idempotency_key VARCHAR(255) NOT NULL COMMENT '业务幂等键，全表唯一',
    reference_type VARCHAR(100) NULL COMMENT '可选的业务关联类型，用于跨任务检索',
    reference_id VARCHAR(255) NULL COMMENT '可选的业务关联标识，与业务关联类型成对出现',
    payload_hash VARCHAR(128) NOT NULL COMMENT 'JSON 负载的 SHA-256 摘要，用于快速校验载荷一致性',
    envelope_json MEDIUMTEXT NOT NULL COMMENT '完整的版本化消息信封 JSON',
    status SMALLINT NOT NULL DEFAULT 0
        COMMENT '任务状态：0-等待投递，1-投递中，2-已投递，3-等待投递重试，4-死信终态，5-执行中，6-执行成功，7-执行失败等待消息重投，8-投递结果未知等待重试',
    dispatch_attempt INTEGER NOT NULL DEFAULT 0 COMMENT '已抢占的投递次数，首次投递抢占时记为 1',
    execution_attempt INTEGER NOT NULL DEFAULT 0 COMMENT '已抢占的执行次数，首次执行抢占时记为 1',
    generation INTEGER NOT NULL DEFAULT 0 COMMENT '重新入队代际，初始为 0，每次重新入队递增',
    next_attempt_at TIMESTAMP(6) NULL COMMENT '下一次允许投递的 UTC 时刻，仅等待投递重试状态必填',
    lease_token CHAR(36) CHARACTER SET ascii COLLATE ascii_bin NULL
        COMMENT '当前投递或执行租约令牌，用于防止过期执行者回写状态',
    lease_until TIMESTAMP(6) NULL COMMENT '当前投递或执行租约到期的 UTC 时刻，过期后可被其他实例接管',
    last_error VARCHAR(2000) NULL COMMENT '最近一次投递或执行失败的错误摘要，最长 2000 字符',
    progress_json MEDIUMTEXT NULL COMMENT '业务处理器最近一次上报的任务进度 JSON',
    dispatched_at TIMESTAMP(6) NULL COMMENT '消息已投递或首次被消费端接收的 UTC 时刻',
    completed_at TIMESTAMP(6) NULL COMMENT '任务执行成功或进入死信终态的 UTC 时刻',
    created_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) COMMENT '记录创建的 UTC 时刻',
    updated_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) COMMENT '记录最后更新的 UTC 时刻',
    PRIMARY KEY (task_id),
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
    ),
    KEY idx_async_task_outbox_pending_claim (status, created_at, task_id),
    KEY idx_async_task_outbox_retry_claim (status, next_attempt_at, created_at, task_id),
    KEY idx_async_task_outbox_lease_claim (status, lease_until, created_at, task_id),
    KEY idx_async_task_outbox_reference (reference_type, reference_id, created_at, task_id),
    KEY idx_async_task_outbox_cleanup (completed_at, task_id, status)
) ENGINE = InnoDB
  DEFAULT CHARACTER SET = utf8mb4
  COLLATE = utf8mb4_bin
  COMMENT = '异步任务发件箱，记录任务投递与消费执行的完整生命周期';
