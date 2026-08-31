package com.zaviwayne.asynctask.autoconfigure.observability;

import com.zaviwayne.asynctask.core.AsyncTaskObserver;
import io.micrometer.core.instrument.MeterRegistry;

import java.util.Objects;

/**
 * Micrometer 异步任务生命周期观察器。
 *
 * @since 2026-08-27
 */
public final class MicrometerAsyncTaskObserver implements AsyncTaskObserver {
    /**
     * 任务入队指标名称。
     */
    private static final String ENQUEUED_METER_NAME = "async.task.enqueued";

    /**
     * 任务投递指标名称。
     */
    private static final String DISPATCH_METER_NAME = "async.task.dispatch";

    /**
     * 任务执行指标名称。
     */
    private static final String EXECUTION_METER_NAME = "async.task.execution";

    /**
     * 死信指标名称。
     */
    private static final String DEAD_LETTER_METER_NAME = "async.task.dead.letter";

    /**
     * 租约接管指标名称。
     */
    private static final String LEASE_RECOVERED_METER_NAME = "async.task.lease.recovered";

    /**
     * 重新入队指标名称。
     */
    private static final String REQUEUED_METER_NAME = "async.task.requeued";

    /**
     * 清理指标名称。
     */
    private static final String CLEANED_METER_NAME = "async.task.cleaned";

    /**
     * 目标主题标签名。
     */
    private static final String DESTINATION_TAG_NAME = "destination";

    /**
     * 任务类型标签名。
     */
    private static final String TASK_TYPE_TAG_NAME = "task.type";

    /**
     * 结果标签名。
     */
    private static final String OUTCOME_TAG_NAME = "outcome";

    /**
     * 阶段标签名。
     */
    private static final String PHASE_TAG_NAME = "phase";

    /**
     * 成功标签值。
     */
    private static final String SUCCESS_TAG_VALUE = "success";

    /**
     * 失败标签值。
     */
    private static final String FAILURE_TAG_VALUE = "failure";

    /**
     * 重试标签值。
     */
    private static final String RETRY_TAG_VALUE = "retry";

    /**
     * 死信标签值。
     */
    private static final String DEAD_TAG_VALUE = "dead";

    /**
     * 投递阶段标签值。
     */
    private static final String DISPATCH_PHASE_TAG_VALUE = "dispatch";

    /**
     * 执行阶段标签值。
     */
    private static final String EXECUTION_PHASE_TAG_VALUE = "execution";

    /**
     * 指标注册表。
     */
    private final MeterRegistry meterRegistry;

    /**
     * 创建 Micrometer 异步任务生命周期观察器。
     *
     * @param meterRegistry 指标注册表
     */
    public MicrometerAsyncTaskObserver(MeterRegistry meterRegistry) {
        this.meterRegistry = Objects.requireNonNull(meterRegistry, "指标注册表不能为空");
    }

    @Override
    public void onEnqueued(String destination, String taskType) {
        increment(ENQUEUED_METER_NAME, destination, taskType, OUTCOME_TAG_NAME, SUCCESS_TAG_VALUE);
    }

    @Override
    public void onDispatchSucceeded(String destination, String taskType) {
        increment(DISPATCH_METER_NAME, destination, taskType, OUTCOME_TAG_NAME, SUCCESS_TAG_VALUE);
    }

    @Override
    public void onDispatchFailed(String destination, String taskType, boolean terminal) {
        String outcome = terminal ? DEAD_TAG_VALUE : RETRY_TAG_VALUE;
        increment(DISPATCH_METER_NAME, destination, taskType, OUTCOME_TAG_NAME, outcome);
    }

    @Override
    public void onExecutionSucceeded(String destination, String taskType) {
        increment(EXECUTION_METER_NAME, destination, taskType, OUTCOME_TAG_NAME, SUCCESS_TAG_VALUE);
    }

    @Override
    public void onExecutionFailed(String destination, String taskType) {
        increment(EXECUTION_METER_NAME, destination, taskType, OUTCOME_TAG_NAME, FAILURE_TAG_VALUE);
    }

    @Override
    public void onDeadLetter(String destination, String taskType) {
        increment(DEAD_LETTER_METER_NAME, destination, taskType, OUTCOME_TAG_NAME, DEAD_TAG_VALUE);
    }

    @Override
    public void onDispatchLeaseRecovered(String destination, String taskType) {
        increment(
                LEASE_RECOVERED_METER_NAME,
                destination,
                taskType,
                PHASE_TAG_NAME,
                DISPATCH_PHASE_TAG_VALUE);
    }

    @Override
    public void onExecutionLeaseRecovered(String destination, String taskType) {
        increment(
                LEASE_RECOVERED_METER_NAME,
                destination,
                taskType,
                PHASE_TAG_NAME,
                EXECUTION_PHASE_TAG_VALUE);
    }

    @Override
    public void onRequeued(String destination, String taskType) {
        increment(REQUEUED_METER_NAME, destination, taskType, OUTCOME_TAG_NAME, SUCCESS_TAG_VALUE);
    }

    @Override
    public void onCleaned(int count) {
        meterRegistry.counter(CLEANED_METER_NAME).increment(count);
    }

    private void increment(String name,
                           String destination,
                           String taskType,
                           String outcomeTag,
                           String outcome) {
        meterRegistry.counter(
                name,
                DESTINATION_TAG_NAME, destination,
                TASK_TYPE_TAG_NAME, taskType,
                outcomeTag, outcome).increment();
    }
}
