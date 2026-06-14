package io.micrometrics.ingestion.database.device;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.Row;

import java.util.List;

public class DeviceRepository {

    private final CqlSession session;
    private final PreparedStatement findAllDevices;
    private final PreparedStatement findByApiKey;

    public DeviceRepository(CqlSession session) {
        this.session = session;
        this.findAllDevices = session.prepare("SELECT * FROM devices");
        this.findByApiKey = session.prepare("SELECT * FROM devices WHERE api_key = ?");
    }

    public List<Device> findAllDevices() {
        return session.execute(findAllDevices.bind())
                .all()
                .stream()
                .map(this::toDevice)
                .toList();
    }

    public Device findByApiKey(String apiKey) {
        var row = session.execute(findByApiKey.bind(apiKey)).one();
        return row != null ? toDevice(row) : null;
    }

    private Device toDevice(Row row) {
        return new Device(
                row.getLong("device_id"),
                row.getString("name"),
                row.getString("api_key"),
                row.getString("firmware_version"),
                row.getInstant("registered_at"),
                row.getInstant("last_seen_at"),
                row.getBoolean("active")
        );
    }
}
