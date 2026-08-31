package com.zaviwayne.asynctask.jdbc;

/**
 * 异步任务 JDBC 数据库方言。
 *
 * @since 2026-08-26
 */
public interface AsyncTaskJdbcDialect {
    /**
     * 获取支持的数据库产品名称。
     *
     * @return 数据库产品名称
     */
    String databaseProductName();

    /**
     * 获取 outbox 幂等插入 SQL。
     *
     * @return 使用命名参数的插入 SQL
     */
    String insertOutboxSql();

    /**
     * 将 UUID 文本转换为当前数据库驱动需要的参数类型。
     *
     * @param value UUID 文本
     * @return JDBC 参数值
     */
    Object uuidParameter(String value);

    /**
     * 获取无破坏性的建表脚本路径。
     *
     * @return classpath 资源路径
     */
    String schemaResource();
}
