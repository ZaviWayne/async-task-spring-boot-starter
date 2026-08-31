package com.zaviwayne.asynctask.core;

import java.time.Instant;

/**
 * 异步任务运行统计。
 *
 * @param backlogCount    尚未进入终态的任务数量
 * @param runningCount    正在执行的任务数量
 * @param deadCount       死信终态任务数量
 * @param oldestBacklogAt 最早积压任务的创建时间，可为空
 * @since 2026-08-27
 */
public record AsyncTaskStatistics(long backlogCount,
                                  long runningCount,
                                  long deadCount,
                                  Instant oldestBacklogAt) {
}
