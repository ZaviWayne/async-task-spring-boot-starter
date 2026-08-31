package com.zaviwayne.asynctask.core;

/**
 * 异步任务传输异常。
 *
 * @since 2026-08-26
 */
public class AsyncTaskTransportException extends AsyncTaskException {
    /**
     * 是否无法确定消息已经投递。
     */
    private final boolean deliveryUncertain;

    /**
     * 创建异步任务传输异常。
     *
     * @param message 异常消息
     * @param cause   原始异常
     */
    public AsyncTaskTransportException(String message, Throwable cause) {
        this(message, cause, false);
    }

    /**
     * 创建带投递结果标识的异步任务传输异常。
     *
     * @param message           异常消息
     * @param cause             原始异常
     * @param deliveryUncertain 是否无法确定消息已经投递
     */
    public AsyncTaskTransportException(String message, Throwable cause, boolean deliveryUncertain) {
        super(message, cause);
        this.deliveryUncertain = deliveryUncertain;
    }

    /**
     * 判断消息投递结果是否未知。
     *
     * @return 无法确认消息是否已经投递时返回 true
     */
    public boolean isDeliveryUncertain() {
        return deliveryUncertain;
    }
}
