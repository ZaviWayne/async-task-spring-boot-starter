package com.zaviwayne.asynctask.jdbc;

import java.time.Duration;
import java.util.Objects;

/**
 * 指数退避策略。
 *
 * @since 2026-08-26
 */
public final class ExponentialBackoffPolicy {
    /**
     * 最大执行次数。
     */
    private final int maxAttempts;

    /**
     * 首次退避时间。
     */
    private final Duration initialDelay;

    /**
     * 最大退避时间。
     */
    private final Duration maxDelay;

    /**
     * 创建指数退避策略。
     *
     * @param maxAttempts  最大执行次数
     * @param initialDelay 首次退避时间
     * @param maxDelay     最大退避时间
     */
    public ExponentialBackoffPolicy(int maxAttempts, Duration initialDelay, Duration maxDelay) {
        if (maxAttempts <= 0) {
            throw new IllegalArgumentException("最大执行次数必须大于 0");
        }
        this.initialDelay = requirePositive(initialDelay, "首次退避时间");
        this.maxDelay = requirePositive(maxDelay, "最大退避时间");
        if (this.initialDelay.compareTo(this.maxDelay) > 0) {
            throw new IllegalArgumentException("首次退避时间不能大于最大退避时间");
        }
        this.maxAttempts = maxAttempts;
    }

    /**
     * 判断当前执行次数是否已经耗尽。
     *
     * @param attempt 已执行次数，从 1 开始
     * @return 已耗尽时返回 true
     */
    public boolean isExhausted(int attempt) {
        return attempt >= maxAttempts;
    }

    /**
     * 获取最大执行次数。
     *
     * @return 最大执行次数
     */
    public int maxAttempts() {
        return maxAttempts;
    }

    /**
     * 计算当前失败后的等待时间。
     *
     * @param attempt 已执行次数，从 1 开始
     * @return 退避时间
     */
    public Duration nextDelay(int attempt) {
        if (attempt <= 0) {
            throw new IllegalArgumentException("执行次数必须大于 0");
        }
        Duration delay = initialDelay;
        for (int index = 1; index < attempt && delay.compareTo(maxDelay) < 0; index++) {
            try {
                delay = delay.multipliedBy(2);
            } catch (ArithmeticException exception) {
                return maxDelay;
            }
            if (delay.compareTo(maxDelay) > 0) {
                return maxDelay;
            }
        }
        return delay;
    }

    private static Duration requirePositive(Duration duration, String fieldName) {
        Objects.requireNonNull(duration, fieldName + "不能为空");
        if (duration.isZero() || duration.isNegative()) {
            throw new IllegalArgumentException(fieldName + "必须大于 0");
        }
        return duration;
    }
}
