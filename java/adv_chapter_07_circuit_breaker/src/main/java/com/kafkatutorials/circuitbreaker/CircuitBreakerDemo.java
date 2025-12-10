package com.kafkatutorials.circuitbreaker;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * Demonstration of circuit breaker pattern with Kafka producer.
 * 
 * Shows:
 * - Normal operation (CLOSED state)
 * - Failures causing circuit to open
 * - Fast rejection when circuit is open
 * - Automatic recovery through HALF_OPEN state
 */
public class CircuitBreakerDemo {
    
    private static final Logger logger = LoggerFactory.getLogger(CircuitBreakerDemo.class);
    
    private static final String TOPIC = "circuit-breaker-demo";
    private static int messageCount = 0;
    
    public static void main(String[] args) {
        System.out.println("╔════════════════════════════════════════════════════════╗");
        System.out.println("║    Circuit Breaker Pattern Demo                       ║");
        System.out.println("╚════════════════════════════════════════════════════════╝");
        System.out.println();
        
        // Configuration
        String bootstrapServers = System.getenv().getOrDefault(
            "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092");
        
        System.out.println("Configuration:");
        System.out.println("  Topic: " + TOPIC);
        System.out.println("  Bootstrap Servers: " + bootstrapServers);
        System.out.println();
        
        // Create circuit breaker config
        CircuitBreakerConfig config = CircuitBreakerConfig.builder()
            .failureThreshold(5)
            .successThreshold(2)
            .timeout(Duration.ofSeconds(10))  // Shorter for demo
            .build();
        
        System.out.println("Circuit Breaker Config:");
        System.out.println("  " + config);
        System.out.println();
        
        // Create producer properties
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 5000);
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 10000);
        
        // Create circuit breaker producer
        try (CircuitBreakerProducer<String, String> producer = 
                new CircuitBreakerProducer<>(props, config, "demo-producer")) {
            
            // Phase 1: Normal operation
            runPhase1NormalOperation(producer);
            
            // Phase 2: Simulate failures
            runPhase2Failures(producer);
            
            // Phase 3: Circuit open - requests rejected
            runPhase3CircuitOpen(producer);
            
            // Phase 4: Wait for timeout
            runPhase4WaitForTimeout(producer, config);
            
            // Phase 5: Half-open recovery
            runPhase5HalfOpenRecovery(producer);
            
            // Phase 6: Circuit closed again
            runPhase6CircuitClosed(producer);
            
            // Summary
            System.out.println("╔════════════════════════════════════════════════════════╗");
            System.out.println("║    Demo Complete!                                      ║");
            System.out.println("╚════════════════════════════════════════════════════════╝");
            System.out.println();
            
            System.out.println(producer.getCircuitBreakerStatus());
            producer.printMetrics();
            
        } catch (Exception e) {
            logger.error("Demo failed", e);
        }
    }
    
    private static void runPhase1NormalOperation(CircuitBreakerProducer<String, String> producer) {
        System.out.println("═══ Phase 1: Normal Operation (CLOSED) ═══");
        System.out.println();
        
        for (int i = 0; i < 3; i++) {
            sendMessage(producer, "normal-operation-" + i, true);
            sleep(500);
        }
        
        System.out.println();
    }
    
    private static void runPhase2Failures(CircuitBreakerProducer<String, String> producer) {
        System.out.println("═══ Phase 2: Simulating Failures ═══");
        System.out.println("(Imagine Kafka is down)");
        System.out.println();
        
        // Simulate failures by using invalid bootstrap servers temporarily
        // In real scenario, Kafka would be down
        
        for (int i = 0; i < 6; i++) {
            sendMessage(producer, "failure-scenario-" + i, false);
            sleep(500);
        }
        
        System.out.println();
    }
    
    private static void runPhase3CircuitOpen(CircuitBreakerProducer<String, String> producer) {
        System.out.println("═══ Phase 3: Circuit OPEN - Fast Rejection ═══");
        System.out.println();
        
        for (int i = 0; i < 3; i++) {
            long startTime = System.currentTimeMillis();
            sendMessage(producer, "rejected-" + i, false);
            long elapsedTime = System.currentTimeMillis() - startTime;
            System.out.println("  → Rejection time: " + elapsedTime + "ms (instant!)");
            sleep(500);
        }
        
        System.out.println();
    }
    
    private static void runPhase4WaitForTimeout(
            CircuitBreakerProducer<String, String> producer,
            CircuitBreakerConfig config) {
        System.out.println("═══ Phase 4: Waiting for Timeout ═══");
        System.out.println("Waiting " + config.getTimeout().getSeconds() + " seconds for circuit to try HALF_OPEN...");
        System.out.println();
        
        sleep(config.getTimeout().toMillis() + 1000);
        
        System.out.println("Timeout expired. Circuit should transition to HALF_OPEN on next request.");
        System.out.println();
    }
    
    private static void runPhase5HalfOpenRecovery(CircuitBreakerProducer<String, String> producer) {
        System.out.println("═══ Phase 5: Testing Recovery (HALF_OPEN) ═══");
        System.out.println();
        
        for (int i = 0; i < 3; i++) {
            sendMessage(producer, "recovery-test-" + i, true);
            sleep(500);
        }
        
        System.out.println();
    }
    
    private static void runPhase6CircuitClosed(CircuitBreakerProducer<String, String> producer) {
        System.out.println("═══ Phase 6: Circuit Restored (CLOSED) ═══");
        System.out.println();
        
        for (int i = 0; i < 2; i++) {
            sendMessage(producer, "back-to-normal-" + i, true);
            sleep(500);
        }
        
        System.out.println();
    }
    
    private static void sendMessage(
            CircuitBreakerProducer<String, String> producer, 
            String value, 
            boolean shouldSucceed) {
        messageCount++;
        ProducerRecord<String, String> record = new ProducerRecord<>(
            TOPIC,
            "key-" + messageCount,
            value
        );
        
        System.out.println("[" + messageCount + "] Sending: " + value);
        
        try {
            RecordMetadata metadata = producer.sendWithCircuitBreaker(record);
            System.out.println("  ✅ SUCCESS - partition: " + metadata.partition() + 
                             ", offset: " + metadata.offset());
            
        } catch (CircuitBreakerException e) {
            System.out.println("  🔴 REJECTED - Circuit is " + e.getState());
            
        } catch (ExecutionException e) {
            System.out.println("  ❌ FAILED - " + e.getCause().getClass().getSimpleName());
            
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            System.out.println("  ⚠️  INTERRUPTED");
        }
        
        // Print circuit breaker state
        System.out.println("  Circuit State: " + producer.getCircuitBreakerState());
    }
    
    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
