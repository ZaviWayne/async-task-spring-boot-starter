package com.zaviwayne.asynctask.jdbc;

import com.zaviwayne.asynctask.core.AsyncTaskEnvelope;

/**
 * Outbox 投递行数据。
 *
 * @since 2026-08-31
 */
record OutboxRow(AsyncTaskEnvelope envelope, int status, int dispatchAttempt) {
}
