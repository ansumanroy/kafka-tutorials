package com.kafkatutorials.performance;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Holds the results of a producer performance benchmark.
 */
public class BenchmarkResult {
    private final String configName;
    private final int messageCount;
    private final int messageSizeBytes;
    private final long totalDurationMs;
    private final List<Long> latencies;
    private final int successCount;
    private final int failureCount;
    
    public BenchmarkResult(String configName, int messageCount, int messageSizeBytes, 
                          long totalDurationMs, List<Long> latencies,
                          int successCount, int failureCount) {
        this.configName = configName;
        this.messageCount = messageCount;
        this.messageSizeBytes = messageSizeBytes;
        this.totalDurationMs = totalDurationMs;
        this.latencies = new ArrayList<>(latencies);
        this.successCount = successCount;
        this.failureCount = failureCount;
    }
    
    // Getters
    public String getConfigName() { return configName; }
    public int getMessageCount() { return messageCount; }
    public int getMessageSizeBytes() { return messageSizeBytes; }
    public long getTotalDurationMs() { return totalDurationMs; }
    public int getSuccessCount() { return successCount; }
    public int getFailureCount() { return failureCount; }
    
    // Calculated metrics
    public double getThroughputMessagesPerSec() {
        return (double) successCount / (totalDurationMs / 1000.0);
    }
    
    public double getThroughputMBPerSec() {
        double bytesPerSec = (successCount * messageSizeBytes) / (totalDurationMs / 1000.0);
        return bytesPerSec / (1024 * 1024);
    }
    
    public long getAvgLatencyMs() {
        if (latencies.isEmpty()) return 0;
        return latencies.stream().mapToLong(Long::longValue).sum() / latencies.size();
    }
    
    public long getP50LatencyMs() {
        return getPercentile(50);
    }
    
    public long getP95LatencyMs() {
        return getPercentile(95);
    }
    
    public long getP99LatencyMs() {
        return getPercentile(99);
    }
    
    public long getMaxLatencyMs() {
        if (latencies.isEmpty()) return 0;
        return Collections.max(latencies);
    }
    
    private long getPercentile(int percentile) {
        if (latencies.isEmpty()) return 0;
        List<Long> sorted = new ArrayList<>(latencies);
        Collections.sort(sorted);
        int index = (int) Math.ceil(percentile / 100.0 * sorted.size()) - 1;
        return sorted.get(Math.max(0, Math.min(index, sorted.size() - 1)));
    }
    
    /**
     * Print detailed benchmark results.
     */
    public void print() {
        System.out.println("\n╔═══════════════════════════════════════════════════════════╗");
        System.out.println("║  Benchmark Results: " + String.format("%-36s", configName) + "║");
        System.out.println("╚═══════════════════════════════════════════════════════════╝");
        
        System.out.println("\n📊 Throughput Metrics:");
        System.out.printf("  • Messages sent:        %,d\n", successCount);
        System.out.printf("  • Failed:               %,d\n", failureCount);
        System.out.printf("  • Duration:             %,d ms (%.2f sec)\n", totalDurationMs, totalDurationMs / 1000.0);
        System.out.printf("  • Throughput:           %,.2f msg/sec\n", getThroughputMessagesPerSec());
        System.out.printf("  • Throughput:           %.2f MB/sec\n", getThroughputMBPerSec());
        
        System.out.println("\n⏱️  Latency Metrics:");
        System.out.printf("  • Average latency:      %d ms\n", getAvgLatencyMs());
        System.out.printf("  • P50 latency:          %d ms\n", getP50LatencyMs());
        System.out.printf("  • P95 latency:          %d ms\n", getP95LatencyMs());
        System.out.printf("  • P99 latency:          %d ms\n", getP99LatencyMs());
        System.out.printf("  • Max latency:          %d ms\n", getMaxLatencyMs());
        
        System.out.println();
    }
    
    /**
     * Print compact summary for comparison.
     */
    public void printSummary() {
        System.out.printf("%-25s | %,10.2f msg/sec | %,6.2f MB/sec | %5d ms (p95) | %5d ms (avg)\n",
            configName,
            getThroughputMessagesPerSec(),
            getThroughputMBPerSec(),
            getP95LatencyMs(),
            getAvgLatencyMs()
        );
    }
}
