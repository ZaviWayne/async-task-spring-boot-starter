package com.zaviwayne.asynctask.jdbc;

import com.zaviwayne.asynctask.core.AsyncTaskEnvelope;

import java.time.Instant;

/**
 * Outbox 执行租约行数据。
 *
 * @since 2026-08-31
 */
record ExecutionRow(AsyncTaskEnvelope envelope, int status, Instant leaseUntil) {
}
