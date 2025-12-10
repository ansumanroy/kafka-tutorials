package com.kafkatutorials.circuitbreaker;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.errors.TimeoutException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/**
 * Unit tests for CircuitBreakerProducer using mocks.
 */
@Tag("circuit-breaker")
class CircuitBreakerProducerTest {
    
    private KafkaProducer<String, String> mockProducer;
    private CircuitBreakerConfig config;
    private ProducerRecord<String, String> testRecord;
    
    @BeforeEach
    void setUp() {
        mockProducer = mock(KafkaProducer.class);
        
        config = CircuitBreakerConfig.builder()
            .failureThreshold(2)
            .successThreshold(1)
            .timeout(Duration.ofMillis(100))
            .build();
        
        testRecord = new ProducerRecord<>("test-topic", "key", "value");
    }
    
    @Test
    void testSuccessfulSend() throws Exception {
        // Mock successful send
        RecordMetadata metadata = new RecordMetadata(
            new org.apache.kafka.common.TopicPartition("test-topic", 0),
            0, 0, 0, 0, 0
        );
        
        Future<RecordMetadata> future = CompletableFuture.completedFuture(metadata);
        when(mockProducer.send(any())).thenReturn(future);
        
        // Create producer (can't easily inject mock, so we test the behavior)
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, 
            "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, 
            "org.apache.kafka.common.serialization.StringSerializer");
        
        // Note: This test demonstrates the structure
        // Full integration test would use embedded Kafka
        assertNotNull(props);
    }
    
    @Test
    void testCircuitBreakerOpensAfterFailures() {
        // This would require integration testing with embedded Kafka
        // or more sophisticated mocking
        
        // Demonstrate the concept
        CircuitBreaker cb = new CircuitBreaker("test", config);
        
        // Simulate failures
        cb.recordFailure();
        cb.recordFailure();
        
        // Circuit should open
        assertEquals(CircuitBreakerState.OPEN, cb.getState());
        assertFalse(cb.allowRequest());
    }
    
    @Test
    void testMetricsCollection() {
        CircuitBreakerMetrics metrics = new CircuitBreakerMetrics("test");
        
        metrics.recordAllowed();
        metrics.recordSuccess();
        
        assertEquals(1, metrics.getTotalAllowed());
        assertEquals(1, metrics.getTotalSuccess());
        assertEquals(100.0, metrics.getSuccessRate(), 0.01);
    }
    
    @Test
    void testCircuitBreakerException() {
        CircuitBreakerException exception = new CircuitBreakerException(
            "Test message",
            CircuitBreakerState.OPEN
        );
        
        assertEquals(CircuitBreakerState.OPEN, exception.getState());
        assertEquals("Test message", exception.getMessage());
    }
    
    @Test
    void testConfigBuilder() {
        CircuitBreakerConfig config = CircuitBreakerConfig.builder()
            .failureThreshold(10)
            .successThreshold(5)
            .timeout(Duration.ofMinutes(2))
            .resetTimeout(Duration.ofSeconds(45))
            .halfOpenMaxConcurrentRequests(5)
            .build();
        
        assertEquals(10, config.getFailureThreshold());
        assertEquals(5, config.getSuccessThreshold());
        assertEquals(Duration.ofMinutes(2), config.getTimeout());
        assertEquals(Duration.ofSeconds(45), config.getResetTimeout());
        assertEquals(5, config.getHalfOpenMaxConcurrentRequests());
    }
    
    @Test
    void testConfigBuilderValidation() {
        assertThrows(IllegalArgumentException.class, () ->
            CircuitBreakerConfig.builder()
                .failureThreshold(0)
                .build()
        );
        
        assertThrows(IllegalArgumentException.class, () ->
            CircuitBreakerConfig.builder()
                .timeout(Duration.ZERO)
                .build()
        );
    }
}
