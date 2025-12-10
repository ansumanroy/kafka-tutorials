package com.kafkatutorials.circuitbreaker;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Thread-safe circuit breaker implementation.
 * 
 * Protects against cascading failures by failing fast when a threshold
 * of consecutive failures is reached.
 * 
 * States:
 * - CLOSED: Normal operation, requests pass through
 * - OPEN: Circuit is open, requests are rejected immediately
 * - HALF_OPEN: Testing recovery, limited requests allowed
 */
public class CircuitBreaker {
    
    private static final Logger logger = LoggerFactory.getLogger(CircuitBreaker.class);
    
    private final String name;
    private final CircuitBreakerConfig config;
    private final AtomicReference<CircuitBreakerState> state;
    private final AtomicInteger failureCount;
    private final AtomicInteger successCount;
    private final AtomicInteger halfOpenConcurrentRequests;
    private final AtomicReference<Instant> lastFailureTime;
    private final AtomicReference<Instant> openedAt;
    
    public CircuitBreaker(String name, CircuitBreakerConfig config) {
        this.name = name;
        this.config = config;
        this.state = new AtomicReference<>(CircuitBreakerState.CLOSED);
        this.failureCount = new AtomicInteger(0);
        this.successCount = new AtomicInteger(0);
        this.halfOpenConcurrentRequests = new AtomicInteger(0);
        this.lastFailureTime = new AtomicReference<>();
        this.openedAt = new AtomicReference<>();
    }
    
    /**
     * Check if a request should be allowed.
     * 
     * @return true if request should proceed, false if it should be rejected
     */
    public boolean allowRequest() {
        CircuitBreakerState currentState = state.get();
        
        switch (currentState) {
            case CLOSED:
                return handleClosedState();
                
            case OPEN:
                return handleOpenState();
                
            case HALF_OPEN:
                return handleHalfOpenState();
                
            default:
                return false;
        }
    }
    
    /**
     * Record a successful operation.
     */
    public void recordSuccess() {
        CircuitBreakerState currentState = state.get();
        
        switch (currentState) {
            case CLOSED:
                // Reset failure count on success
                failureCount.set(0);
                lastFailureTime.set(null);
                break;
                
            case HALF_OPEN:
                int successes = successCount.incrementAndGet();
                halfOpenConcurrentRequests.decrementAndGet();
                
                logger.info("[{}] Success in HALF_OPEN state ({}/{})", 
                    name, successes, config.getSuccessThreshold());
                
                if (successes >= config.getSuccessThreshold()) {
                    transitionToClosed();
                }
                break;
                
            case OPEN:
                // Shouldn't happen, but handle gracefully
                logger.warn("[{}] Received success in OPEN state", name);
                break;
        }
    }
    
    /**
     * Record a failed operation.
     */
    public void recordFailure() {
        CircuitBreakerState currentState = state.get();
        
        switch (currentState) {
            case CLOSED:
                int failures = failureCount.incrementAndGet();
                lastFailureTime.set(Instant.now());
                
                logger.warn("[{}] Failure in CLOSED state ({}/{})", 
                    name, failures, config.getFailureThreshold());
                
                if (failures >= config.getFailureThreshold()) {
                    transitionToOpen();
                }
                break;
                
            case HALF_OPEN:
                halfOpenConcurrentRequests.decrementAndGet();
                logger.warn("[{}] Failure in HALF_OPEN state - reopening circuit", name);
                transitionToOpen();
                break;
                
            case OPEN:
                // Already open, update opened time
                openedAt.set(Instant.now());
                break;
        }
    }
    
    /**
     * Get current circuit breaker state.
     */
    public CircuitBreakerState getState() {
        return state.get();
    }
    
    /**
     * Get current failure count.
     */
    public int getFailureCount() {
        return failureCount.get();
    }
    
    /**
     * Get current success count (in half-open state).
     */
    public int getSuccessCount() {
        return successCount.get();
    }
    
    /**
     * Reset circuit breaker to CLOSED state (for testing/manual intervention).
     */
    public void reset() {
        state.set(CircuitBreakerState.CLOSED);
        failureCount.set(0);
        successCount.set(0);
        halfOpenConcurrentRequests.set(0);
        lastFailureTime.set(null);
        openedAt.set(null);
        logger.info("[{}] Circuit breaker manually reset to CLOSED", name);
    }
    
    /**
     * Get circuit breaker status as string.
     */
    public String getStatus() {
        CircuitBreakerState currentState = state.get();
        StringBuilder sb = new StringBuilder();
        sb.append(String.format("Circuit Breaker '%s' Status:\n", name));
        sb.append(String.format("  State: %s\n", currentState));
        sb.append(String.format("  Failure Count: %d\n", failureCount.get()));
        sb.append(String.format("  Success Count: %d\n", successCount.get()));
        
        if (currentState == CircuitBreakerState.OPEN) {
            Instant opened = openedAt.get();
            if (opened != null) {
                long secondsSinceOpen = java.time.Duration.between(opened, Instant.now()).getSeconds();
                long secondsRemaining = config.getTimeout().getSeconds() - secondsSinceOpen;
                sb.append(String.format("  Time until half-open: %ds\n", Math.max(0, secondsRemaining)));
            }
        }
        
        return sb.toString();
    }
    
    // Private helper methods
    
    private boolean handleClosedState() {
        // Check if we should reset failure count due to time elapsed
        Instant lastFailure = lastFailureTime.get();
        if (lastFailure != null && failureCount.get() > 0) {
            long secondsSinceFailure = java.time.Duration.between(lastFailure, Instant.now()).getSeconds();
            if (secondsSinceFailure >= config.getResetTimeout().getSeconds()) {
                logger.info("[{}] Resetting failure count after {}s of no failures", 
                    name, secondsSinceFailure);
                failureCount.set(0);
                lastFailureTime.set(null);
            }
        }
        
        return true; // Allow request in CLOSED state
    }
    
    private boolean handleOpenState() {
        Instant opened = openedAt.get();
        if (opened == null) {
            // Shouldn't happen, but handle gracefully
            opened = Instant.now();
            openedAt.set(opened);
        }
        
        long secondsSinceOpen = java.time.Duration.between(opened, Instant.now()).getSeconds();
        
        if (secondsSinceOpen >= config.getTimeout().getSeconds()) {
            transitionToHalfOpen();
            return true; // Allow the test request
        }
        
        return false; // Reject request in OPEN state
    }
    
    private boolean handleHalfOpenState() {
        int concurrent = halfOpenConcurrentRequests.get();
        
        if (concurrent >= config.getHalfOpenMaxConcurrentRequests()) {
            return false; // Reject if too many concurrent requests in half-open
        }
        
        halfOpenConcurrentRequests.incrementAndGet();
        return true; // Allow limited requests in HALF_OPEN state
    }
    
    private void transitionToOpen() {
        state.set(CircuitBreakerState.OPEN);
        openedAt.set(Instant.now());
        failureCount.set(0);
        successCount.set(0);
        logger.error("[{}] Circuit breaker OPENING (failure threshold exceeded)", name);
    }
    
    private void transitionToHalfOpen() {
        state.set(CircuitBreakerState.HALF_OPEN);
        failureCount.set(0);
        successCount.set(0);
        halfOpenConcurrentRequests.set(0);
        logger.info("[{}] Circuit breaker transitioning to HALF_OPEN (timeout expired)", name);
    }
    
    private void transitionToClosed() {
        state.set(CircuitBreakerState.CLOSED);
        failureCount.set(0);
        successCount.set(0);
        halfOpenConcurrentRequests.set(0);
        lastFailureTime.set(null);
        openedAt.set(null);
        logger.info("[{}] Circuit breaker CLOSING (success threshold met)", name);
    }
}
