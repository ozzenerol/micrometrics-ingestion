package io.micrometrics.ingestion.server;

import io.micrometrics.ingestion.config.model.ConfigModel;
import io.micrometrics.ingestion.database.device.DeviceCache;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;

@Slf4j
@RequiredArgsConstructor
public class IngestionServer {

    private final ConfigModel config;
    private final ExecutorService executor;
    private final SpoolWriter spoolWriter;
    private final DeviceCache deviceCache;

    private static volatile boolean running = true;

    public void start() {
        log.info("Attempting to start server on port {}", config.port());

        try (var serverSocket = new ServerSocket(config.port())) {
            log.info("Started server on port {}", config.port());

            while (running) {
                var socket = serverSocket.accept();
                log.debug("Accepted connection from {}", socket.getInetAddress());
                executor.submit(() -> handleClient(socket));
            }

        } catch (IOException e) {
            throw new IOError(e);
        }
    }

    public void handleClient(Socket socket) {
        try (socket; var in = new DataInputStream(new BufferedInputStream(socket.getInputStream()))) {
            while (!socket.isClosed()) {
                SensorFrameModel frame = SensorFrameModel.decode(in);

                var device = deviceCache.getByApiKey(frame.apiKey());
                if (device == null) {
                    device = deviceCache.getDeviceRepository().findByApiKey(frame.apiKey());
                }

                if (device == null || !device.active()) {
                    log.warn("Rejected frame from {}: invalid or inactive api key", socket.getInetAddress());
                    return;
                }

                log.debug("Device {} metric {} = {}", frame.deviceId(), frame.metricType(), frame.value());
                spoolWriter.submit(frame);
            }
        } catch (EOFException e) {
            log.debug("Client disconnected: {}", socket.getInetAddress());
        } catch (IOException e) {
            log.error("Error reading from {}", socket.getInetAddress(), e);
        }
    }
}
