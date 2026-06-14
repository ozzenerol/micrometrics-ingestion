package io.micrometrics.ingestion.database.device;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@Slf4j
public class DeviceCache {

    @Getter
    private final DeviceRepository deviceRepository;
    private final Map<String, Device> cache = new ConcurrentHashMap<>();

    public DeviceCache(DeviceRepository deviceRepository) {
        this.deviceRepository = deviceRepository;
    }

    public void start() {
        sync(); // populate before accepting connections
        log.info("Starting device cache refresh every 30s");
        var scheduler = Executors.newSingleThreadScheduledExecutor(Thread.ofVirtual().factory());
        scheduler.scheduleAtFixedRate(this::sync, 30, 30, TimeUnit.SECONDS);
    }

    private void sync() {
        List<Device> devices = deviceRepository.findAllDevices();
        var updated = new ConcurrentHashMap<String, Device>(devices.size() * 2);
        devices.forEach(device -> updated.put(device.apiKey(), device));
        cache.clear();
        cache.putAll(updated);
        log.info("Device cache synced, {} devices loaded", cache.size());
    }

    public Device getByApiKey(String apiKey) {
        return cache.get(apiKey);
    }
}