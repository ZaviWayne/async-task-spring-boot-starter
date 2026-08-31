package com.zaviwayne.asynctask.jdbc;

import com.zaviwayne.asynctask.core.AsyncTaskEnvelope;

/**
 * Outbox 投递租约明细。
 *
 * @param envelope          任务信封
 * @param attempt           当前投递次数
 * @param deliveryUncertain 是否继承投递结果未知语义
 * @since 2026-08-31
 */
record OutboxClaim(AsyncTaskEnvelope envelope, int attempt, boolean deliveryUncertain) {
}
