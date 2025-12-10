package com.kafkatutorials.circuitbreaker;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Metrics collector for circuit breaker operations.
 */
public class CircuitBreakerMetrics {
    
    private final MeterRegistry registry;
    private final String circuitBreakerName;
    
    // Counters
    private final Counter allowedCounter;
    private final Counter rejectedCounter;
    private final Counter successCounter;
    private final Counter failureCounter;
    private final Counter stateTransitionCounter;
    
    // Timers
    private final Timer operationTimer;
    
    // Manual tracking
    private final AtomicLong totalAllowed;
    private final AtomicLong totalRejected;
    private final AtomicLong totalSuccess;
    private final AtomicLong totalFailure;
    
    public CircuitBreakerMetrics(String circuitBreakerName) {
        this(circuitBreakerName, new SimpleMeterRegistry());
    }
    
    public CircuitBreakerMetrics(String circuitBreakerName, MeterRegistry registry) {
        this.circuitBreakerName = circuitBreakerName;
        this.registry = registry;
        
        // Initialize counters
        this.allowedCounter = Counter.builder("circuit.breaker.requests.allowed")
            .tag("circuit.breaker", circuitBreakerName)
            .description("Number of allowed requests")
            .register(registry);
        
        this.rejectedCounter = Counter.builder("circuit.breaker.requests.rejected")
            .tag("circuit.breaker", circuitBreakerName)
            .description("Number of rejected requests")
            .register(registry);
        
        this.successCounter = Counter.builder("circuit.breaker.operations.success")
            .tag("circuit.breaker", circuitBreakerName)
            .description("Number of successful operations")
            .register(registry);
        
        this.failureCounter = Counter.builder("circuit.breaker.operations.failure")
            .tag("circuit.breaker", circuitBreakerName)
            .description("Number of failed operations")
            .register(registry);
        
        this.stateTransitionCounter = Counter.builder("circuit.breaker.state.transitions")
            .tag("circuit.breaker", circuitBreakerName)
            .description("Number of state transitions")
            .register(registry);
        
        // Initialize timer
        this.operationTimer = Timer.builder("circuit.breaker.operation.duration")
            .tag("circuit.breaker", circuitBreakerName)
            .description("Operation duration")
            .register(registry);
        
        // Manual tracking
        this.totalAllowed = new AtomicLong(0);
        this.totalRejected = new AtomicLong(0);
        this.totalSuccess = new AtomicLong(0);
        this.totalFailure = new AtomicLong(0);
    }
    
    public void recordAllowed() {
        allowedCounter.increment();
        totalAllowed.incrementAndGet();
    }
    
    public void recordRejected() {
        rejectedCounter.increment();
        totalRejected.incrementAndGet();
    }
    
    public void recordSuccess() {
        successCounter.increment();
        totalSuccess.incrementAndGet();
    }
    
    public void recordFailure() {
        failureCounter.increment();
        totalFailure.incrementAndGet();
    }
    
    public void recordStateTransition() {
        stateTransitionCounter.increment();
    }
    
    public Timer.Sample startTimer() {
        return Timer.start(registry);
    }
    
    public void stopTimer(Timer.Sample sample) {
        sample.stop(operationTimer);
    }
    
    public long getTotalAllowed() {
        return totalAllowed.get();
    }
    
    public long getTotalRejected() {
        return totalRejected.get();
    }
    
    public long getTotalSuccess() {
        return totalSuccess.get();
    }
    
    public long getTotalFailure() {
        return totalFailure.get();
    }
    
    public double getSuccessRate() {
        long total = totalSuccess.get() + totalFailure.get();
        if (total == 0) return 0.0;
        return (double) totalSuccess.get() / total * 100.0;
    }
    
    public double getAverageOperationTimeMs() {
        return operationTimer.mean(TimeUnit.MILLISECONDS);
    }
    
    public void printSummary() {
        System.out.println("\n=== Circuit Breaker Metrics: " + circuitBreakerName + " ===");
        System.out.printf("Total Allowed:      %,d%n", getTotalAllowed());
        System.out.printf("Total Rejected:     %,d%n", getTotalRejected());
        System.out.printf("Total Success:      %,d%n", getTotalSuccess());
        System.out.printf("Total Failure:      %,d%n", getTotalFailure());
        System.out.printf("Success Rate:       %.1f%%%n", getSuccessRate());
        System.out.printf("Avg Operation Time: %.2f ms%n", getAverageOperationTimeMs());
        
        long timeSaved = getTotalRejected() * 5000; // Assuming 5s timeout
        System.out.printf("Time Saved:         %,d ms (%.1f seconds)%n", 
            timeSaved, timeSaved / 1000.0);
        System.out.println("================================================\n");
    }
}
