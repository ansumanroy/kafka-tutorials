package com.kafkatutorials.circuitbreaker;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for CircuitBreaker.
 */
@Tag("circuit-breaker")
class CircuitBreakerTest {
    
    private CircuitBreaker circuitBreaker;
    private CircuitBreakerConfig config;
    
    @BeforeEach
    void setUp() {
        config = CircuitBreakerConfig.builder()
            .failureThreshold(3)
            .successThreshold(2)
            .timeout(Duration.ofSeconds(1))
            .build();
        
        circuitBreaker = new CircuitBreaker("test-circuit", config);
    }
    
    @Test
    void testInitialStateClosed() {
        assertEquals(CircuitBreakerState.CLOSED, circuitBreaker.getState());
        assertTrue(circuitBreaker.allowRequest());
    }
    
    @Test
    void testCircuitOpensAfterThreshold() {
        // Record failures up to threshold
        for (int i = 0; i < config.getFailureThreshold(); i++) {
            circuitBreaker.recordFailure();
        }
        
        // Circuit should be open
        assertEquals(CircuitBreakerState.OPEN, circuitBreaker.getState());
        assertFalse(circuitBreaker.allowRequest());
    }
    
    @Test
    void testCircuitRemainsClosedBelowThreshold() {
        // Record failures below threshold
        for (int i = 0; i < config.getFailureThreshold() - 1; i++) {
            circuitBreaker.recordFailure();
        }
        
        // Circuit should still be closed
        assertEquals(CircuitBreakerState.CLOSED, circuitBreaker.getState());
        assertTrue(circuitBreaker.allowRequest());
    }
    
    @Test
    void testSuccessResetsFailureCount() {
        // Record some failures
        circuitBreaker.recordFailure();
        circuitBreaker.recordFailure();
        
        // Record success
        circuitBreaker.recordSuccess();
        
        // Failure count should be reset
        assertEquals(0, circuitBreaker.getFailureCount());
        assertEquals(CircuitBreakerState.CLOSED, circuitBreaker.getState());
    }
    
    @Test
    void testTransitionToHalfOpenAfterTimeout() throws InterruptedException {
        // Open the circuit
        for (int i = 0; i < config.getFailureThreshold(); i++) {
            circuitBreaker.recordFailure();
        }
        
        assertEquals(CircuitBreakerState.OPEN, circuitBreaker.getState());
        
        // Wait for timeout
        Thread.sleep(config.getTimeout().toMillis() + 100);
        
        // Next request should transition to half-open
        assertTrue(circuitBreaker.allowRequest());
        assertEquals(CircuitBreakerState.HALF_OPEN, circuitBreaker.getState());
    }
    
    @Test
    void testHalfOpenClosesAfterSuccessThreshold() throws InterruptedException {
        // Open circuit
        for (int i = 0; i < config.getFailureThreshold(); i++) {
            circuitBreaker.recordFailure();
        }
        
        // Wait and transition to half-open
        Thread.sleep(config.getTimeout().toMillis() + 100);
        circuitBreaker.allowRequest();
        
        assertEquals(CircuitBreakerState.HALF_OPEN, circuitBreaker.getState());
        
        // Record successes to close circuit
        for (int i = 0; i < config.getSuccessThreshold(); i++) {
            circuitBreaker.recordSuccess();
        }
        
        assertEquals(CircuitBreakerState.CLOSED, circuitBreaker.getState());
    }
    
    @Test
    void testHalfOpenReopensOnFailure() throws InterruptedException {
        // Open circuit
        for (int i = 0; i < config.getFailureThreshold(); i++) {
            circuitBreaker.recordFailure();
        }
        
        // Transition to half-open
        Thread.sleep(config.getTimeout().toMillis() + 100);
        circuitBreaker.allowRequest();
        
        assertEquals(CircuitBreakerState.HALF_OPEN, circuitBreaker.getState());
        
        // Record failure - should reopen circuit
        circuitBreaker.recordFailure();
        
        assertEquals(CircuitBreakerState.OPEN, circuitBreaker.getState());
    }
    
    @Test
    void testReset() {
        // Open circuit
        for (int i = 0; i < config.getFailureThreshold(); i++) {
            circuitBreaker.recordFailure();
        }
        
        assertEquals(CircuitBreakerState.OPEN, circuitBreaker.getState());
        
        // Reset
        circuitBreaker.reset();
        
        assertEquals(CircuitBreakerState.CLOSED, circuitBreaker.getState());
        assertEquals(0, circuitBreaker.getFailureCount());
        assertEquals(0, circuitBreaker.getSuccessCount());
    }
    
    @Test
    void testConcurrentRequests() throws InterruptedException {
        int numThreads = 10;
        Thread[] threads = new Thread[numThreads];
        
        for (int i = 0; i < numThreads; i++) {
            threads[i] = new Thread(() -> {
                for (int j = 0; j < 100; j++) {
                    circuitBreaker.allowRequest();
                    if (j % 10 == 0) {
                        circuitBreaker.recordSuccess();
                    }
                }
            });
            threads[i].start();
        }
        
        for (Thread thread : threads) {
            thread.join();
        }
        
        // No assertions - just checking for thread safety
        assertNotNull(circuitBreaker.getState());
    }
}
