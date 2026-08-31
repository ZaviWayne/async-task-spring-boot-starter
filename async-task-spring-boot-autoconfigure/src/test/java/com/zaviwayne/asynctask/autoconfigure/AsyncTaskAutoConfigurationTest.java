package com.zaviwayne.asynctask.autoconfigure;

import com.zaviwayne.asynctask.autoconfigure.jdbc.AsyncTaskJdbcAutoConfiguration;
import com.zaviwayne.asynctask.autoconfigure.kafka.AsyncTaskKafkaAutoConfiguration;
import com.zaviwayne.asynctask.autoconfigure.kafka.AsyncTaskKafkaContainers;
import com.zaviwayne.asynctask.autoconfigure.observability.AsyncTaskHealthAutoConfiguration;
import com.zaviwayne.asynctask.autoconfigure.observability.AsyncTaskObservabilityAutoConfiguration;
import com.zaviwayne.asynctask.autoconfigure.properties.AsyncTaskProperties;
import com.zaviwayne.asynctask.autoconfigure.properties.AsyncTaskTopicBinding;
import com.zaviwayne.asynctask.autoconfigure.retention.AsyncTaskCleanupJob;
import com.zaviwayne.asynctask.autoconfigure.retention.AsyncTaskRetentionAutoConfiguration;
import com.zaviwayne.asynctask.core.*;
import com.zaviwayne.asynctask.jdbc.AsyncTaskDispatcher;
import com.zaviwayne.asynctask.jdbc.AsyncTaskJdbcDialect;
import com.zaviwayne.asynctask.jdbc.JdbcTaskStore;
import org.apache.kafka.clients.admin.NewTopic;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.health.contributor.HealthIndicator;
import org.springframework.boot.health.contributor.Status;
import org.springframework.boot.kafka.autoconfigure.KafkaAutoConfiguration;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.support.JdbcTransactionManager;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import tools.jackson.databind.ObjectMapper;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.Collection;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;
import static org.mockito.Mockito.*;

class AsyncTaskAutoConfigurationTest {
    private final DataSource testDataSource = dataSource();

    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(
                    AsyncTaskAutoConfiguration.class,
                    AsyncTaskJdbcAutoConfiguration.class,
                    AsyncTaskObservabilityAutoConfiguration.class,
                    AsyncTaskHealthAutoConfiguration.class,
                    AsyncTaskRetentionAutoConfiguration.class,
                    AsyncTaskKafkaAutoConfiguration.class))
            .withBean(DataSource.class, () -> testDataSource)
            .withBean(ObjectMapper.class, ObjectMapper::new)
            .withPropertyValues(
                    "async-task.database.platform=postgresql",
                    "async-task.kafka.enabled=false");

    @Test
    void shouldConfigureJdbcInfrastructureWithoutKafka() {
        contextRunner.run(context -> {
            assertThat(context).hasSingleBean(TaskPayloadSerializer.class);
            assertThat(context).hasSingleBean(AsyncTaskContentLimits.class);
            assertThat(context).hasSingleBean(AsyncTaskJdbcDialect.class);
            assertThat(context).hasSingleBean(JdbcTaskStore.class);
            assertThat(context).hasSingleBean(AsyncTaskAdmin.class);
            assertThat(context).hasSingleBean(AsyncTaskObserver.class);
            assertThat(context).hasSingleBean(AsyncTaskEnqueuer.class);
            assertThat(context).hasSingleBean(HealthIndicator.class);
            assertThat(context).doesNotHaveBean(AsyncTaskCleanupJob.class);
            assertThat(context).doesNotHaveBean(AsyncTaskTransport.class);
            assertThat(context.getBean(AsyncTaskJdbcDialect.class).databaseProductName())
                    .isEqualTo("PostgreSQL");
        });
    }

    @Test
    void shouldConfigureContentLimits() {
        contextRunner
                .withPropertyValues(
                        "async-task.outbox.max-envelope-bytes=123",
                        "async-task.outbox.max-progress-bytes=456")
                .run(context -> assertThat(context.getBean(AsyncTaskContentLimits.class))
                        .extracting(
                                AsyncTaskContentLimits::maxEnvelopeBytes,
                                AsyncTaskContentLimits::maxProgressBytes)
                        .containsExactly(123, 456));
    }

    @Test
    void shouldUseApplicationJdbcTransactionManagerMatchingDataSource() {
        DataSourceTransactionManager primaryTransactionManager = mock(DataSourceTransactionManager.class);
        DataSourceTransactionManager matchingTransactionManager = mock(DataSourceTransactionManager.class);
        when(primaryTransactionManager.getDataSource()).thenReturn(mock(DataSource.class));
        when(matchingTransactionManager.getDataSource()).thenReturn(testDataSource);

        contextRunner
                .withBean(
                        "primaryTransactionManager",
                        DataSourceTransactionManager.class,
                        () -> primaryTransactionManager,
                        beanDefinition -> beanDefinition.setPrimary(true))
                .withBean(
                        "matchingTransactionManager",
                        DataSourceTransactionManager.class,
                        () -> matchingTransactionManager)
                .run(context -> {
                    JdbcTaskStore taskStore = context.getBean(JdbcTaskStore.class);
                    TransactionTemplate transactionTemplate = (TransactionTemplate)
                            ReflectionTestUtils.getField(taskStore, "transactionTemplate");

                    assertThat(transactionTemplate).isNotNull();
                    assertThat(transactionTemplate.getTransactionManager())
                            .isSameAs(matchingTransactionManager);
                });
    }

    @Test
    void shouldCreateDedicatedTransactionManagerWhenNoManagerMatchesDataSource() {
        DataSourceTransactionManager transactionManager = mock(DataSourceTransactionManager.class);
        when(transactionManager.getDataSource()).thenReturn(mock(DataSource.class));

        contextRunner
                .withBean(DataSourceTransactionManager.class, () -> transactionManager)
                .run(context -> {
                    JdbcTaskStore taskStore = context.getBean(JdbcTaskStore.class);
                    TransactionTemplate transactionTemplate = (TransactionTemplate)
                            ReflectionTestUtils.getField(taskStore, "transactionTemplate");

                    assertThat(transactionTemplate).isNotNull();
                    assertThat(transactionTemplate.getTransactionManager())
                            .isInstanceOf(JdbcTransactionManager.class)
                            .isNotSameAs(transactionManager);
                });
    }

    @Test
    void shouldRejectMultipleTransactionManagersForAsyncTaskDataSource() {
        DataSourceTransactionManager firstTransactionManager = mock(DataSourceTransactionManager.class);
        DataSourceTransactionManager secondTransactionManager = mock(DataSourceTransactionManager.class);
        when(firstTransactionManager.getDataSource()).thenReturn(testDataSource);
        when(secondTransactionManager.getDataSource()).thenReturn(testDataSource);

        contextRunner
                .withBean("firstTransactionManager", DataSourceTransactionManager.class,
                        () -> firstTransactionManager)
                .withBean("secondTransactionManager", DataSourceTransactionManager.class,
                        () -> secondTransactionManager)
                .run(context -> {
                    assertThat(context).hasFailed();
                    assertThat(context.getStartupFailure()).hasRootCauseMessage(
                            "存在多个绑定到异步任务数据源的 JDBC 事务管理器");
                });
    }

    @Test
    void shouldIgnoreNonJdbcTransactionManager() {
        PlatformTransactionManager nonJdbcTransactionManager = mock(PlatformTransactionManager.class);

        contextRunner
                .withBean(PlatformTransactionManager.class, () -> nonJdbcTransactionManager)
                .run(context -> {
                    JdbcTaskStore taskStore = context.getBean(JdbcTaskStore.class);
                    TransactionTemplate transactionTemplate = (TransactionTemplate)
                            ReflectionTestUtils.getField(taskStore, "transactionTemplate");

                    assertThat(transactionTemplate).isNotNull();
                    assertThat(transactionTemplate.getTransactionManager())
                            .isInstanceOf(JdbcTransactionManager.class)
                            .isNotSameAs(nonJdbcTransactionManager);
                });
    }

    @Test
    void shouldRejectConfiguredPlatformThatDiffersFromJdbcMetadata() {
        contextRunner
                .withPropertyValues("async-task.database.platform=mysql")
                .run(context -> {
                    assertThat(context).hasFailed();
                    assertThat(context.getStartupFailure())
                            .hasRootCauseMessage(
                                    "配置的数据库平台与 JDBC 元数据不一致: configured=MYSQL, detected=POSTGRESQL");
                });
    }

    @Test
    void shouldBindPerTopicConfiguration() {
        contextRunner
                .withPropertyValues(
                        "async-task.kafka.bindings[0].topic=resume-events",
                        "async-task.kafka.bindings[0].dead-letter-topic=resume-events-dead",
                        "async-task.kafka.bindings[0].parking-topic=resume-events-parking",
                        "async-task.kafka.bindings[0].consumer-group=resume-workers",
                        "async-task.kafka.bindings[0].concurrency=3",
                        "async-task.kafka.bindings[0].partitions=6",
                        "async-task.kafka.bindings[0].replicas=2",
                        "async-task.kafka.bindings[1].topic=job-events")
                .run(context -> {
                    AsyncTaskProperties properties = context.getBean(AsyncTaskProperties.class);

                    assertThat(properties.kafka().bindings()).hasSize(2);
                    assertThat(properties.kafka().bindings().getFirst())
                            .extracting(
                                    AsyncTaskTopicBinding::topic,
                                    AsyncTaskTopicBinding::deadLetterTopic,
                                    AsyncTaskTopicBinding::parkingTopic,
                                    AsyncTaskTopicBinding::consumerGroup,
                                    AsyncTaskTopicBinding::concurrency,
                                    AsyncTaskTopicBinding::partitions,
                                    AsyncTaskTopicBinding::replicas)
                            .containsExactly(
                                    "resume-events", "resume-events-dead", "resume-events-parking",
                                    "resume-workers", 3, 6, 2);
                    assertThat(properties.kafka().bindings().getLast())
                            .extracting(
                                    AsyncTaskTopicBinding::topic,
                                    AsyncTaskTopicBinding::deadLetterTopic,
                                    AsyncTaskTopicBinding::parkingTopic,
                                    AsyncTaskTopicBinding::consumerGroup,
                                    AsyncTaskTopicBinding::concurrency,
                                    AsyncTaskTopicBinding::partitions,
                                    AsyncTaskTopicBinding::replicas)
                            .containsExactly(
                                    "job-events", "job-events.DLT", "job-events.DLT.PARKING",
                                    "async-task", 1, 3, 1);
                });
    }

    @Test
    void shouldReportUpWhenTaskStorageIsAvailable() {
        JdbcTaskStore taskStore = mock(JdbcTaskStore.class);

        contextRunner
                .withBean(JdbcTaskStore.class, () -> taskStore)
                .run(context -> {
                    assertThat(context.getBean(
                            "asyncTaskHealthIndicator", HealthIndicator.class).health().getStatus())
                            .isEqualTo(Status.UP);
                    verify(taskStore).checkHealth();
                });
    }

    @Test
    void shouldReportDownWhenTaskStorageCheckFails() {
        JdbcTaskStore taskStore = mock(JdbcTaskStore.class);
        doThrow(new IllegalStateException("数据库不可用")).when(taskStore).checkHealth();

        contextRunner
                .withBean(JdbcTaskStore.class, () -> taskStore)
                .run(context -> assertThat(context.getBean(
                        "asyncTaskHealthIndicator", HealthIndicator.class).health().getStatus())
                        .isEqualTo(Status.DOWN));
    }

    @Test
    void shouldConfigureKafkaInfrastructureAfterSpringBootKafkaAutoConfiguration() {
        contextRunner
                .withConfiguration(AutoConfigurations.of(KafkaAutoConfiguration.class))
                .withPropertyValues(
                        "async-task.kafka.enabled=true",
                        "async-task.outbox.enabled=false",
                        "spring.kafka.bootstrap-servers=localhost:9092")
                .run(context -> {
                    assertThat(context).hasSingleBean(KafkaOperations.class);
                    assertThat(context).hasSingleBean(ConsumerFactory.class);
                    assertThat(context).hasSingleBean(AsyncTaskTransport.class);
                    assertThat(context).hasSingleBean(AsyncTaskKafkaContainers.class);
                    assertThat(context).hasSingleBean(AsyncTaskDispatcher.class);
                });
    }

    @Test
    @SuppressWarnings("unchecked")
    void shouldDeclareBusinessAndDeadLetterTopics() {
        contextRunner
                .withBean(KafkaOperations.class, () -> mock(KafkaOperations.class))
                .withBean(ConsumerFactory.class, () -> mock(ConsumerFactory.class))
                .withBean(AsyncTaskKafkaContainers.class, () -> mock(AsyncTaskKafkaContainers.class))
                .withBean(AsyncTaskDispatcher.class, () -> mock(AsyncTaskDispatcher.class))
                .withPropertyValues(
                        "async-task.kafka.enabled=true",
                        "async-task.kafka.auto-create-topics=true",
                        "async-task.outbox.enabled=false",
                        "async-task.kafka.bindings[0].topic=task-events",
                        "async-task.kafka.bindings[0].dead-letter-topic=task-events-dead",
                        "async-task.kafka.bindings[0].parking-topic=task-events-parking",
                        "async-task.kafka.bindings[0].consumer-group=workers",
                        "async-task.kafka.bindings[0].concurrency=2",
                        "async-task.kafka.bindings[0].partitions=6",
                        "async-task.kafka.bindings[0].replicas=2")
                .run(context -> {
                    KafkaAdmin.NewTopics declarations = context.getBean(
                            "asyncTaskKafkaTopics", KafkaAdmin.NewTopics.class);
                    Collection<NewTopic> topics = (Collection<NewTopic>) ReflectionTestUtils.getField(
                            declarations, "newTopics");

                    assertThat(topics)
                            .extracting(NewTopic::name, NewTopic::numPartitions, NewTopic::replicationFactor)
                            .containsExactlyInAnyOrder(
                                    tuple("task-events", 6, (short) 2),
                                    tuple("task-events-dead", 6, (short) 2),
                                    tuple("task-events-parking", 6, (short) 2));
                });
    }

    private static DataSource dataSource() {
        DataSource dataSource = mock(DataSource.class);
        Connection connection = mock(Connection.class);
        DatabaseMetaData metadata = mock(DatabaseMetaData.class);
        try {
            when(dataSource.getConnection()).thenReturn(connection);
            when(connection.getMetaData()).thenReturn(metadata);
            when(metadata.getDatabaseProductName()).thenReturn("PostgreSQL");
            when(metadata.getDatabaseMajorVersion()).thenReturn(17);
        } catch (SQLException exception) {
            throw new IllegalStateException("构造测试数据源失败", exception);
        }
        return dataSource;
    }
}
