package io.micrometrics.ingestion.server;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
public class SpoolWriter {

    private static final int MIN_BATCH_SIZE = 10_000;
    private static final int MAX_BATCH_SIZE = 100_000;
    private static final long MAX_WAIT_MS   = 200;
    private static final int RECORD_SIZE    = 8 + 8 + 2 + 4; // 22 bytes
    private static final int NUM_WRITERS    = Runtime.getRuntime().availableProcessors();

    private final BlockingQueue<SensorFrameModel> queue = new LinkedBlockingQueue<>();
    private final AtomicLong fileSeq = new AtomicLong();
    private final Path spoolDir;

    public SpoolWriter(Path spoolDir) {
        this.spoolDir = spoolDir;
    }

    public void submit(SensorFrameModel frame) {
        queue.add(frame);
    }

    public void start() {
        recoverSpoolDir();
        log.info("Spool writer starting {} writer threads, writing to {}, next seq={}", NUM_WRITERS, spoolDir, fileSeq.get());
        for (int i = 0; i < NUM_WRITERS; i++) {
            Thread.ofPlatform().name("spool-writer-" + i).daemon(true).start(this::runWriter);
        }
    }

    private void recoverSpoolDir() {
        long maxSeq = -1;
        try (var stream = Files.list(spoolDir)) {
            for (Path p : stream.toList()) {
                String name = p.getFileName().toString();
                // delete orphaned .bin files — they were never fully written
                if (name.startsWith("spool-") && name.endsWith(".bin") && !name.endsWith(".bin.done")) {
                    Files.deleteIfExists(p);
                    log.warn("Deleted orphaned spool file {}", name);
                    continue;
                }
                // track highest sequence across .bin.done files
                if (name.startsWith("spool-") && name.endsWith(".bin.done")) {
                    String seqStr = name.substring("spool-".length(), name.length() - ".bin.done".length());
                    try { maxSeq = Math.max(maxSeq, Long.parseLong(seqStr)); } catch (NumberFormatException ignored) {}
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        fileSeq.set(maxSeq + 1);
    }

    private void runWriter() {
        // one direct buffer per thread — avoids an extra kernel copy on FileChannel.write()
        ByteBuffer buffer = ByteBuffer.allocateDirect(MAX_BATCH_SIZE * RECORD_SIZE);
        List<SensorFrameModel> batch = new ArrayList<>(MAX_BATCH_SIZE);

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
                flush(batch, buffer);
            } catch (InterruptedException e) {
                log.info("Spool writer interrupted, shutting down");
                Thread.currentThread().interrupt();
            }
        }
    }

    private void flush(List<SensorFrameModel> batch, ByteBuffer buffer) {
        // unique filename via monotonic counter — nanoTime alone can collide across threads
        var file = spoolDir.resolve("spool-" + fileSeq.getAndIncrement() + ".bin");
        log.info("Flushing {} frames to {}", batch.size(), file.getFileName());
        long start = System.currentTimeMillis();

        buffer.clear();
        for (var frame : batch) {
            buffer.putLong(frame.deviceId());
            buffer.putLong(frame.timestamp());
            buffer.putShort(frame.metricType());
            buffer.putFloat(frame.value());
        }
        buffer.flip();

        try (var channel = FileChannel.open(file, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)) {
            while (buffer.hasRemaining()) {
                channel.write(buffer);
            }
        } catch (IOException e) {
            log.error("Failed to write spool file {}", file, e);
            throw new UncheckedIOException(e);
        }

        try {
            var done = file.resolveSibling(file.getFileName() + ".done");
            Files.move(file, done);
            long elapsed = System.currentTimeMillis() - start;
            log.info("Wrote {} frames ({} bytes) to {} in {}ms",
                    batch.size(), (long) batch.size() * RECORD_SIZE, done.getFileName(), elapsed);
        } catch (IOException e) {
            log.error("Failed to rename spool file {}", file, e);
            throw new UncheckedIOException(e);
        }
    }
}
