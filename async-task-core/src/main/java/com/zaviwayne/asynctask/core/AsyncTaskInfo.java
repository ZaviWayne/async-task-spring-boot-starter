package com.zaviwayne.asynctask.core;

import java.time.Instant;
import java.util.UUID;

/**
 * 异步任务只读信息。
 *
 * @param taskId            任务 ID
 * @param destination       目标通道
 * @param taskType          任务类型
 * @param schemaVersion     消息结构版本
 * @param idempotencyKey    幂等键
 * @param referenceType     业务关联类型，可为空
 * @param referenceId       业务关联标识，可为空
 * @param status            当前状态
 * @param dispatchAttempts  已投递尝试次数
 * @param executionAttempts 已执行尝试次数
 * @param nextAttemptAt     下次允许投递时间，可为空
 * @param lastError         最近一次错误，可为空
 * @param progressJson      最近一次进度 JSON，可为空
 * @param dispatchedAt      首次投递时间，可为空
 * @param completedAt       终态完成时间，可为空
 * @param createdAt         创建时间
 * @param updatedAt         更新时间
 * @since 2026-08-27
 */
public record AsyncTaskInfo(UUID taskId,
                            String destination,
                            String taskType,
                            int schemaVersion,
                            String idempotencyKey,
                            String referenceType,
                            String referenceId,
                            AsyncTaskStatus status,
                            int dispatchAttempts,
                            int executionAttempts,
                            Instant nextAttemptAt,
                            String lastError,
                            String progressJson,
                            Instant dispatchedAt,
                            Instant completedAt,
                            Instant createdAt,
                            Instant updatedAt) {
}
