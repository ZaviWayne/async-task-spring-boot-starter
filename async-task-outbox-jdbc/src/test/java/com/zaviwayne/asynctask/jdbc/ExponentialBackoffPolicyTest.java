package com.zaviwayne.asynctask.jdbc;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ExponentialBackoffPolicyTest {
    @Test
    void shouldIncreaseDelayAndCapAtMaximum() {
        ExponentialBackoffPolicy policy = new ExponentialBackoffPolicy(
            5, Duration.ofMillis(250), Duration.ofSeconds(1));

        assertThat(policy.nextDelay(1)).isEqualTo(Duration.ofMillis(250));
        assertThat(policy.nextDelay(2)).isEqualTo(Duration.ofMillis(500));
        assertThat(policy.nextDelay(3)).isEqualTo(Duration.ofSeconds(1));
        assertThat(policy.nextDelay(4)).isEqualTo(Duration.ofSeconds(1));
    }

    @Test
    void shouldReportExhaustionAtConfiguredAttempt() {
        ExponentialBackoffPolicy policy = new ExponentialBackoffPolicy(
            3, Duration.ofSeconds(1), Duration.ofMinutes(1));

        assertThat(policy.isExhausted(2)).isFalse();
        assertThat(policy.isExhausted(3)).isTrue();
        assertThat(policy.maxAttempts()).isEqualTo(3);
    }

    @Test
    void shouldRejectInvalidConfiguration() {
        assertThatThrownBy(() -> new ExponentialBackoffPolicy(
            0, Duration.ofSeconds(1), Duration.ofSeconds(2)))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("最大执行次数必须大于 0");
    }
}
