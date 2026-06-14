package io.micrometrics.ingestion;

import com.fasterxml.jackson.core.JsonProcessingException;
import io.micrometrics.ingestion.config.ConfigProvider;
import io.micrometrics.ingestion.config.model.ConfigModel;
import io.micrometrics.ingestion.database.ConnectionProvider;
import io.micrometrics.ingestion.database.device.DeviceCache;
import io.micrometrics.ingestion.database.device.DeviceRepository;
import io.micrometrics.ingestion.server.IngestionServer;
import io.micrometrics.ingestion.server.SpoolWriter;
import lombok.extern.slf4j.Slf4j;

import java.nio.file.Path;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Slf4j
public class IngestionService {

    public static void main(String[] args) throws JsonProcessingException {
        ConfigModel config = ConfigProvider.INSTANCE.config();

        SpoolWriter spoolWriter = new SpoolWriter(Path.of(config.spool().directory()));
        spoolWriter.start();

        DeviceRepository deviceRepository = new DeviceRepository(ConnectionProvider.INSTANCE.session());
        DeviceCache cache = new DeviceCache(deviceRepository);
        cache.start();

        ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
        IngestionServer server = new IngestionServer(config, executor, spoolWriter, cache);
        server.start();
    }
}
