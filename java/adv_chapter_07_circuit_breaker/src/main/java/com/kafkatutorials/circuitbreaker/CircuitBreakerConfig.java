package com.kafkatutorials.circuitbreaker;

import java.time.Duration;

/**
 * Configuration for circuit breaker behavior.
 */
public class CircuitBreakerConfig {
    
    private final int failureThreshold;
    private final int successThreshold;
    private final Duration timeout;
    private final Duration resetTimeout;
    private final int halfOpenMaxConcurrentRequests;
    
    private CircuitBreakerConfig(Builder builder) {
        this.failureThreshold = builder.failureThreshold;
        this.successThreshold = builder.successThreshold;
        this.timeout = builder.timeout;
        this.resetTimeout = builder.resetTimeout;
        this.halfOpenMaxConcurrentRequests = builder.halfOpenMaxConcurrentRequests;
    }
    
    public int getFailureThreshold() {
        return failureThreshold;
    }
    
    public int getSuccessThreshold() {
        return successThreshold;
    }
    
    public Duration getTimeout() {
        return timeout;
    }
    
    public Duration getResetTimeout() {
        return resetTimeout;
    }
    
    public int getHalfOpenMaxConcurrentRequests() {
        return halfOpenMaxConcurrentRequests;
    }
    
    public static Builder builder() {
        return new Builder();
    }
    
    public static CircuitBreakerConfig defaultConfig() {
        return builder().build();
    }
    
    public static class Builder {
        private int failureThreshold = 5;
        private int successThreshold = 2;
        private Duration timeout = Duration.ofSeconds(60);
        private Duration resetTimeout = Duration.ofSeconds(30);
        private int halfOpenMaxConcurrentRequests = 3;
        
        public Builder failureThreshold(int failureThreshold) {
            if (failureThreshold <= 0) {
                throw new IllegalArgumentException("Failure threshold must be positive");
            }
            this.failureThreshold = failureThreshold;
            return this;
        }
        
        public Builder successThreshold(int successThreshold) {
            if (successThreshold <= 0) {
                throw new IllegalArgumentException("Success threshold must be positive");
            }
            this.successThreshold = successThreshold;
            return this;
        }
        
        public Builder timeout(Duration timeout) {
            if (timeout.isNegative() || timeout.isZero()) {
                throw new IllegalArgumentException("Timeout must be positive");
            }
            this.timeout = timeout;
            return this;
        }
        
        public Builder resetTimeout(Duration resetTimeout) {
            if (resetTimeout.isNegative()) {
                throw new IllegalArgumentException("Reset timeout must be non-negative");
            }
            this.resetTimeout = resetTimeout;
            return this;
        }
        
        public Builder halfOpenMaxConcurrentRequests(int max) {
            if (max <= 0) {
                throw new IllegalArgumentException("Max concurrent requests must be positive");
            }
            this.halfOpenMaxConcurrentRequests = max;
            return this;
        }
        
        public CircuitBreakerConfig build() {
            return new CircuitBreakerConfig(this);
        }
    }
    
    @Override
    public String toString() {
        return "CircuitBreakerConfig{" +
               "failureThreshold=" + failureThreshold +
               ", successThreshold=" + successThreshold +
               ", timeout=" + timeout.toSeconds() + "s" +
               ", resetTimeout=" + resetTimeout.toSeconds() + "s" +
               ", halfOpenMaxConcurrentRequests=" + halfOpenMaxConcurrentRequests +
               '}';
    }
}
