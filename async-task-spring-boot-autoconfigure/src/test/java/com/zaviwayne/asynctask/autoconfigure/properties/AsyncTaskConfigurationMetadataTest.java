package com.zaviwayne.asynctask.autoconfigure.properties;

import org.junit.jupiter.api.Test;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.InputStream;
import java.util.LinkedHashSet;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

class AsyncTaskConfigurationMetadataTest {
    /**
     * Spring Boot 配置元数据路径。
     */
    private static final String METADATA_RESOURCE = "META-INF/spring-configuration-metadata.json";

    @Test
    void shouldExposeNestedConfigurationProperties() throws IOException {
        Set<String> propertyNames = configurationPropertyNames();

        assertThat(propertyNames).contains(
                "async-task.database.platform",
                "async-task.database.initialize-schema",
                "async-task.outbox.poll-interval",
                "async-task.outbox.execution-heartbeat-interval",
                "async-task.outbox.max-envelope-bytes",
                "async-task.outbox.max-progress-bytes",
                "async-task.retention.max-batches-per-run",
                "async-task.observability.statistics-cache-duration",
                "async-task.kafka.transaction-enabled",
                "async-task.kafka.dead-letter-max-retries",
                "async-task.kafka.bindings");
    }

    private static Set<String> configurationPropertyNames() throws IOException {
        ClassLoader classLoader = AsyncTaskConfigurationMetadataTest.class.getClassLoader();
        try (InputStream inputStream = classLoader.getResourceAsStream(METADATA_RESOURCE)) {
            assertThat(inputStream).as("Spring Boot 配置元数据").isNotNull();
            JsonNode root = new ObjectMapper().readTree(inputStream);
            Set<String> propertyNames = new LinkedHashSet<>();
            root.get("properties").forEach(property ->
                    propertyNames.add(property.get("name").asText()));
            return propertyNames;
        }
    }
}
