package com.zaviwayne.asynctask.autoconfigure.properties;

/**
 * 异步任务数据库平台。
 *
 * @since 2026-08-26
 */
public enum DatabasePlatform {
    /**
     * 根据 JDBC 元数据自动判断。
     */
    AUTO,

    /**
     * PostgreSQL 13 及以上版本。
     */
    POSTGRESQL,

    /**
     * MySQL 8.0 及以上版本。
     */
    MYSQL
}
