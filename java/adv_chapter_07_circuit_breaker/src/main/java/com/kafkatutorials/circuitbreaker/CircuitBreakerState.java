package com.kafkatutorials.circuitbreaker;

/**
 * Circuit breaker states.
 */
public enum CircuitBreakerState {
    /**
     * Normal operation - requests pass through.
     * Transitions to OPEN when failure threshold is exceeded.
     */
    CLOSED,
    
    /**
     * Circuit is open - all requests are rejected immediately.
     * Transitions to HALF_OPEN after timeout expires.
     */
    OPEN,
    
    /**
     * Testing recovery - limited requests are allowed.
     * Transitions to CLOSED on success threshold, or back to OPEN on failure.
     */
    HALF_OPEN
}
