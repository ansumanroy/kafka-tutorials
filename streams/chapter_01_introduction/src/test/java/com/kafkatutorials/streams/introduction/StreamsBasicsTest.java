package com.kafkatutorials.streams.introduction;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for StreamsBasics using TopologyTestDriver.
 * TopologyTestDriver allows testing without a real Kafka cluster.
 */
class StreamsBasicsTest {
    
    private TopologyTestDriver testDriver;
    private TestInputTopic<String, String> inputTopic;
    private TestOutputTopic<String, Long> outputTopic;
    
    @BeforeEach
    void setup() {
        // Create test configuration
        Properties props = StreamsConfigHelper.getBasicConfig(
            "wordcount-test", 
            "dummy:1234"  // Not used by TopologyTestDriver
        );
        
        // Build topology
        StreamsBuilder builder = StreamsBasics.createWordCountTopology();
        
        // Create test driver
        testDriver = new TopologyTestDriver(builder.build(), props);
        
        // Create test topics
        inputTopic = testDriver.createInputTopic(
            StreamsBasics.INPUT_TOPIC,
            new StringSerializer(),
            new StringSerializer()
        );
        
        outputTopic = testDriver.createOutputTopic(
            StreamsBasics.OUTPUT_TOPIC,
            new StringDeserializer(),
            Serdes.Long().deserializer()
        );
    }
    
    @AfterEach
    void tearDown() {
        if (testDriver != null) {
            testDriver.close();
        }
    }
    
    @Test
    @DisplayName("Should count words in a single line")
    void testSingleLine() {
        // Input
        inputTopic.pipeInput("key1", "hello world hello");
        
        // Verify output - word counts
        assertEquals(2L, outputTopic.readKeyValue().value); // hello=1 (first)
        assertEquals(1L, outputTopic.readKeyValue().value); // world=1
        assertEquals(2L, outputTopic.readKeyValue().value); // hello=2 (updated)
        
        assertTrue(outputTopic.isEmpty());
    }
    
    @Test
    @DisplayName("Should count words across multiple lines")
    void testMultipleLines() {
        // Input
        inputTopic.pipeInput("key1", "kafka streams");
        inputTopic.pipeInput("key2", "hello kafka");
        inputTopic.pipeInput("key3", "streams processing");
        
        // We should get updates for each word
        var results = outputTopic.readKeyValuesToMap();
        
        assertEquals(2L, results.get("kafka"));      // appears twice
        assertEquals(2L, results.get("streams"));    // appears twice
        assertEquals(1L, results.get("hello"));      // appears once
        assertEquals(1L, results.get("processing")); // appears once
    }
    
    @Test
    @DisplayName("Should handle case insensitivity")
    void testCaseInsensitivity() {
        // Input with mixed case
        inputTopic.pipeInput("key1", "Hello HELLO hello");
        
        // All should be counted as "hello"
        assertEquals(1L, outputTopic.readKeyValue().value); // hello=1
        assertEquals(2L, outputTopic.readKeyValue().value); // hello=2
        assertEquals(3L, outputTopic.readKeyValue().value); // hello=3
        
        assertTrue(outputTopic.isEmpty());
    }
    
    @Test
    @DisplayName("Should split on non-word characters")
    void testWordSplitting() {
        // Input with various delimiters
        inputTopic.pipeInput("key1", "hello,world!hello-kafka");
        
        // Should get: hello (twice), world, kafka
        var results = outputTopic.readKeyValuesToMap();
        
        assertEquals(2L, results.get("hello"));
        assertEquals(1L, results.get("world"));
        assertEquals(1L, results.get("kafka"));
    }
    
    @Test
    @DisplayName("Should handle empty input")
    void testEmptyInput() {
        // Input empty string
        inputTopic.pipeInput("key1", "");
        
        // Should produce no output
        assertTrue(outputTopic.isEmpty());
    }
    
    @Test
    @DisplayName("Should update counts incrementally")
    void testIncrementalCounts() {
        // Send same word multiple times
        inputTopic.pipeInput("key1", "kafka");
        assertEquals(1L, outputTopic.readKeyValue().value); // kafka=1
        
        inputTopic.pipeInput("key2", "kafka");
        assertEquals(2L, outputTopic.readKeyValue().value); // kafka=2
        
        inputTopic.pipeInput("key3", "kafka");
        assertEquals(3L, outputTopic.readKeyValue().value); // kafka=3
        
        assertTrue(outputTopic.isEmpty());
    }
}
