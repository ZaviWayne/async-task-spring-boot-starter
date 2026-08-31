package com.zaviwayne.asynctask.jdbc;

import com.zaviwayne.asynctask.core.AsyncTaskEnvelope;

import java.util.UUID;

/**
 * Outbox 幂等身份行数据。
 *
 * @since 2026-08-31
 */
record OutboxIdentity(UUID taskId, AsyncTaskEnvelope envelope) {
}
