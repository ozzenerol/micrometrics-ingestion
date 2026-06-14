package io.micrometrics.ingestion.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometrics.ingestion.config.model.ConfigModel;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.IOException;

@Slf4j
public enum ConfigProvider {
    INSTANCE;

    private final ConfigModel config;

    private static final String CONFIG_PATH = "config.json";

    ConfigProvider() {
        try {
            this.config = new ObjectMapper().readValue(new File(CONFIG_PATH), ConfigModel.class);
        } catch (IOException e) {
            throw new IllegalStateException(String.format("Could not read config file %s: %s", CONFIG_PATH, e.getMessage()), e);
        }
    }

    public ConfigModel config() {
        return config;
    }
}
