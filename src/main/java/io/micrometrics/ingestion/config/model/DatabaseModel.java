package io.micrometrics.ingestion.config.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public record DatabaseModel(
        String host,
        int port,
        String username,
        String password,
        String keyspace,
        String localDataCenter
) {
}
