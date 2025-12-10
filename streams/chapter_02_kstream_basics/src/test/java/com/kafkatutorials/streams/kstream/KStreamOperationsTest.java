package com.kafkatutorials.streams.kstream;

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

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for KStream operations using TopologyTestDriver.
 */
class KStreamOperationsTest {
    
    private TopologyTestDriver testDriver;
    private Properties props;
    
    @BeforeEach
    void setup() {
        props = TransformationHelper.getBasicConfig("test-app", "dummy:1234");
    }
    
    @AfterEach
    void tearDown() {
        if (testDriver != null) {
            testDriver.close();
        }
    }
    
    @Test
    @DisplayName("Filter operation should keep only matching records")
    void testFilter() {
        // Create topology: filter messages longer than 5 characters
        StreamsBuilder builder = new StreamsBuilder();
        builder.<String, String>stream("input")
            .filter((k, v) -> v != null && v.length() > 5)
            .to("output");
        
        testDriver = new TopologyTestDriver(builder.build(), props);
        TestInputTopic<String, String> input = testDriver.createInputTopic(
            "input", new StringSerializer(), new StringSerializer());
        TestOutputTopic<String, String> output = testDriver.createOutputTopic(
            "output", new StringDeserializer(), new StringDeserializer());
        
        // Send test data
        input.pipeInput("key1", "short");   // 5 chars - filtered out
        input.pipeInput("key2", "longer");  // 6 chars - passes
        input.pipeInput("key3", "hi");      // 2 chars - filtered out
        input.pipeInput("key4", "message"); // 7 chars - passes
        
        // Verify output
        List<String> results = output.readValuesToList();
        assertEquals(2, results.size());
        assertEquals("longer", results.get(0));
        assertEquals("message", results.get(1));
    }
    
    @Test
    @DisplayName("Map operation should transform all records")
    void testMap() {
        StreamsBuilder builder = new StreamsBuilder();
        builder.<String, String>stream("input")
            .mapValues(String::toUpperCase)
            .to("output");
        
        testDriver = new TopologyTestDriver(builder.build(), props);
        TestInputTopic<String, String> input = testDriver.createInputTopic(
            "input", new StringSerializer(), new StringSerializer());
        TestOutputTopic<String, String> output = testDriver.createOutputTopic(
            "output", new StringDeserializer(), new StringDeserializer());
        
        input.pipeInput("key1", "hello");
        input.pipeInput("key2", "world");
        
        List<String> results = output.readValuesToList();
        assertEquals(Arrays.asList("HELLO", "WORLD"), results);
    }
    
    @Test
    @DisplayName("FlatMapValues should expand one record into multiple")
    void testFlatMapValues() {
        StreamsBuilder builder = new StreamsBuilder();
        builder.<String, String>stream("input")
            .flatMapValues(value -> Arrays.asList(value.split(" ")))
            .to("output");
        
        testDriver = new TopologyTestDriver(builder.build(), props);
        TestInputTopic<String, String> input = testDriver.createInputTopic(
            "input", new StringSerializer(), new StringSerializer());
        TestOutputTopic<String, String> output = testDriver.createOutputTopic(
            "output", new StringDeserializer(), new StringDeserializer());
        
        input.pipeInput("key1", "hello world");
        input.pipeInput("key2", "kafka streams");
        
        List<String> results = output.readValuesToList();
        assertEquals(Arrays.asList("hello", "world", "kafka", "streams"), results);
    }
    
    @Test
    @DisplayName("FilterMap topology should filter then transform")
    void testFilterMapTopology() {
        testDriver = new TopologyTestDriver(FilterMapDemo.createTopology().build(), props);
        
        TestInputTopic<String, String> input = testDriver.createInputTopic(
            FilterMapDemo.INPUT_TOPIC, new StringSerializer(), new StringSerializer());
        TestOutputTopic<String, String> output = testDriver.createOutputTopic(
            FilterMapDemo.OUTPUT_TOPIC, new StringDeserializer(), new StringDeserializer());
        
        // Test data
        input.pipeInput("1", "short");           // Filtered out (5 chars)
        input.pipeInput("2", "hello world");     // Passes (11 chars) -> [HELLO, WORLD]
        input.pipeInput("3", "hi");              // Filtered out (2 chars)
        input.pipeInput("4", "kafka streams");   // Passes (14 chars) -> [KAFKA, STREAMS]
        
        // Should get 4 words total (2 from each passing message)
        List<String> results = output.readValuesToList();
        assertEquals(4, results.size());
        assertEquals(Arrays.asList("HELLO", "WORLD", "KAFKA", "STREAMS"), results);
    }
    
    @Test
    @DisplayName("Branching should route messages to different streams")
    void testBranching() {
        testDriver = new TopologyTestDriver(BranchingDemo.createTopology().build(), props);
        
        TestInputTopic<String, String> input = testDriver.createInputTopic(
            BranchingDemo.INPUT_TOPIC, new StringSerializer(), new StringSerializer());
        
        TestOutputTopic<String, String> errorOutput = testDriver.createOutputTopic(
            BranchingDemo.ERROR_TOPIC, new StringDeserializer(), new StringDeserializer());
        TestOutputTopic<String, String> warnOutput = testDriver.createOutputTopic(
            BranchingDemo.WARN_TOPIC, new StringDeserializer(), new StringDeserializer());
        TestOutputTopic<String, String> infoOutput = testDriver.createOutputTopic(
            BranchingDemo.INFO_TOPIC, new StringDeserializer(), new StringDeserializer());
        TestOutputTopic<String, String> debugOutput = testDriver.createOutputTopic(
            BranchingDemo.DEBUG_TOPIC, new StringDeserializer(), new StringDeserializer());
        
        // Send messages with different levels
        input.pipeInput("1", "ERROR: Something went wrong");
        input.pipeInput("2", "WARN: This is a warning");
        input.pipeInput("3", "INFO: Just informing you");
        input.pipeInput("4", "DEBUG: Debug information");
        input.pipeInput("5", "ERROR: Another error");
        
        // Verify routing
        List<String> errors = errorOutput.readValuesToList();
        List<String> warns = warnOutput.readValuesToList();
        List<String> infos = infoOutput.readValuesToList();
        List<String> debugs = debugOutput.readValuesToList();
        
        assertEquals(2, errors.size());
        assertEquals(1, warns.size());
        assertEquals(1, infos.size());
        assertEquals(1, debugs.size());
        
        assertTrue(errors.get(0).startsWith("ERROR:"));
        assertTrue(warns.get(0).startsWith("WARN:"));
        assertTrue(infos.get(0).startsWith("INFO:"));
        assertTrue(debugs.get(0).startsWith("DEBUG:"));
    }
    
    @Test
    @DisplayName("Peek should allow side effects without modifying stream")
    void testPeek() {
        StreamsBuilder builder = new StreamsBuilder();
        final StringBuilder sideEffect = new StringBuilder();
        
        builder.<String, String>stream("input")
            .peek((k, v) -> sideEffect.append(v).append(","))
            .mapValues(String::toUpperCase)
            .to("output");
        
        testDriver = new TopologyTestDriver(builder.build(), props);
        TestInputTopic<String, String> input = testDriver.createInputTopic(
            "input", new StringSerializer(), new StringSerializer());
        TestOutputTopic<String, String> output = testDriver.createOutputTopic(
            "output", new StringDeserializer(), new StringDeserializer());
        
        input.pipeInput("1", "hello");
        input.pipeInput("2", "world");
        
        // Side effect should capture original values
        assertEquals("hello,world,", sideEffect.toString());
        
        // Output should have transformed values
        List<String> results = output.readValuesToList();
        assertEquals(Arrays.asList("HELLO", "WORLD"), results);
    }
    
    @Test
    @DisplayName("Chaining operations should work correctly")
    void testChainedOperations() {
        StreamsBuilder builder = new StreamsBuilder();
        builder.<String, String>stream("input")
            .filter((k, v) -> v != null && !v.isEmpty())
            .mapValues(String::toLowerCase)
            .filter((k, v) -> v.length() > 3)
            .mapValues(v -> v + "!")
            .to("output");
        
        testDriver = new TopologyTestDriver(builder.build(), props);
        TestInputTopic<String, String> input = testDriver.createInputTopic(
            "input", new StringSerializer(), new StringSerializer());
        TestOutputTopic<String, String> output = testDriver.createOutputTopic(
            "output", new StringDeserializer(), new StringDeserializer());
        
        input.pipeInput("1", "");         // Filtered by first filter
        input.pipeInput("2", "HELLO");    // Passes -> "hello!"
        input.pipeInput("3", "hi");       // Filtered by second filter (length)
        input.pipeInput("4", "WORLD");    // Passes -> "world!"
        
        List<String> results = output.readValuesToList();
        assertEquals(Arrays.asList("hello!", "world!"), results);
    }
}
