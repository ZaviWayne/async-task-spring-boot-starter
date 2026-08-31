package com.zaviwayne.asynctask.jdbc;

import org.springframework.transaction.support.TransactionSynchronization;

import java.util.Objects;

/**
 * 事务提交后执行指定回调的同步器。
 *
 * @since 2026-08-31
 */
final class AfterCommitTransactionSynchronization implements TransactionSynchronization {
    /**
     * 提交后回调。
     */
    private final Runnable callback;

    /**
     * 创建事务提交后回调同步器。
     *
     * @param callback 提交后回调
     */
    public AfterCommitTransactionSynchronization(Runnable callback) {
        this.callback = Objects.requireNonNull(callback, "事务提交后回调不能为空");
    }

    @Override
    public void afterCommit() {
        callback.run();
    }
}
