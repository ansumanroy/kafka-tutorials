package com.kafkatutorials.circuitbreaker;

/**
 * Exception thrown when circuit breaker rejects a request.
 */
public class CircuitBreakerException extends RuntimeException {
    
    private final CircuitBreakerState state;
    
    public CircuitBreakerException(String message, CircuitBreakerState state) {
        super(message);
        this.state = state;
    }
    
    public CircuitBreakerException(String message, Throwable cause, CircuitBreakerState state) {
        super(message, cause);
        this.state = state;
    }
    
    public CircuitBreakerState getState() {
        return state;
    }
}
