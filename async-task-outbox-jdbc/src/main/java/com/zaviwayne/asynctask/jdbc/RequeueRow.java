package com.zaviwayne.asynctask.jdbc;

import com.zaviwayne.asynctask.core.AsyncTaskEnvelope;

/**
 * Outbox 重新入队行数据。
 *
 * @since 2026-08-31
 */
record RequeueRow(AsyncTaskEnvelope envelope, int status) {
}
