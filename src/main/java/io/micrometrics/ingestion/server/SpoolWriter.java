package io.micrometrics.ingestion.server;

import lombok.extern.slf4j.Slf4j;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

@Slf4j
public class SpoolWriter {

    private static final int MIN_BATCH_SIZE = 10_000;
    private static final int MAX_BATCH_SIZE = 100_000;
    private static final long MAX_WAIT_MS   = 200;

    private final BlockingQueue<SensorFrameModel> queue = new LinkedBlockingQueue<>();
    private final Path spoolDir;

    public SpoolWriter(Path spoolDir) {
        this.spoolDir = spoolDir;
    }

    public void submit(SensorFrameModel frame) {
        queue.add(frame);
    }

    public void start() {
        Thread.ofVirtual().name("spool-writer").start(() -> {
            log.info("Spool writer started, writing to {}", spoolDir);
            var batch = new ArrayList<SensorFrameModel>();
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    batch.clear();
                    SensorFrameModel first = queue.take();
                    batch.add(first);
                    long deadline = System.currentTimeMillis() + MAX_WAIT_MS;
                    while (batch.size() < MIN_BATCH_SIZE) {
                        long remaining = deadline - System.currentTimeMillis();
                        if (remaining <= 0) break;
                        SensorFrameModel frame = queue.poll(remaining, TimeUnit.MILLISECONDS);
                        if (frame == null) break;
                        batch.add(frame);
                    }
                    queue.drainTo(batch, MAX_BATCH_SIZE - batch.size());
                    flush(batch);
                } catch (InterruptedException e) {
                    log.info("Spool writer interrupted, shutting down");
                    Thread.currentThread().interrupt();
                }
            }
        });
    }

    private void flush(List<SensorFrameModel> batch) {
        var file = spoolDir.resolve("spool-" + System.nanoTime() + ".bin");
        log.info("Flushing {} frames to {}", batch.size(), file.getFileName());
        long start = System.currentTimeMillis();
        try (var out = new DataOutputStream(new BufferedOutputStream(Files.newOutputStream(file)))) {
            for (var frame : batch) {
                out.writeLong(frame.deviceId());
                out.writeLong(frame.timestamp());
                out.writeShort(frame.metricType());
                out.writeFloat(frame.value());
            }
        } catch (IOException e) {
            log.error("Failed to write spool file {}", file, e);
            throw new UncheckedIOException(e);
        }
        try {
            var done = file.resolveSibling(file.getFileName() + ".done");
            Files.move(file, done);
            long elapsed = System.currentTimeMillis() - start;
            long sizeBytes = Files.size(done);
            log.info("Wrote {} frames ({} bytes) to {} in {}ms", batch.size(), sizeBytes, done.getFileName(), elapsed);
        } catch (IOException e) {
            log.error("Failed to rename spool file {}", file, e);
            throw new UncheckedIOException(e);
        }
    }
}
