package io.micrometrics.ingestion.config.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public record SpoolModel(
        String directory,
        int maxRows
) {
}
