package io.micrometrics.ingestion.server;

import java.io.DataInputStream;
import java.io.IOException;

public record SensorFrameModel(String apiKey, long deviceId, long timestamp, short metricType, float value) {

    public static SensorFrameModel decode(DataInputStream in) throws IOException {
        String apiKey = in.readUTF();
        long deviceId = in.readLong();
        long timestamp = in.readLong();
        short metricType = in.readShort();
        float value = in.readFloat();
        return new SensorFrameModel(apiKey, deviceId, timestamp, metricType, value);
    }
}
