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
    private final Map<String, Device> deviceCache = new ConcurrentHashMap<>();

    public DeviceCache(DeviceRepository deviceRepository) {
        this.deviceRepository = deviceRepository;
    }

    public void start() {
        log.info("Starting device cache sync every 30s");
        try ( var scheduler = Executors.newSingleThreadScheduledExecutor(Thread.ofVirtual().factory());) {
            scheduler.scheduleAtFixedRate(this::sync, 30, 30, TimeUnit.SECONDS);
        }
    }

    private void sync() {
        List<Device> devices = deviceRepository.findAllDevices();
        var updated = new ConcurrentHashMap<String, Device>();
        devices.forEach(device -> updated.put(device.apiKey(), device));
        deviceCache.clear();
        deviceCache.putAll(updated);
        log.info("Device cache synced, {} devices loaded", deviceCache.size());
    }

    public Device getByApiKey(String apiKey) {
        return deviceCache.get(apiKey);
    }
}