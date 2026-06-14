package io.micrometrics.ingestion.database.device;

import java.time.Instant;

public record Device(
        long deviceId,
        String name,
        String apiKey,
        String firmwareVersion,
        Instant registeredAt,
        Instant lastSeenAt,
        boolean active
) {}
