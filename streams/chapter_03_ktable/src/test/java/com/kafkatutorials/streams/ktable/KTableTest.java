package com.kafkatutorials.streams.ktable;

import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.KTable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for KTable operations.
 */
class KTableTest {
    
    private TopologyTestDriver testDriver;
    private Properties props;
    
    @BeforeEach
    void setup() {
        props = new Properties();
        props.put("application.id", "test");
        props.put("bootstrap.servers", "dummy:1234");
    }
    
    @AfterEach
    void tearDown() {
        if (testDriver != null) {
            testDriver.close();
        }
    }
    
    @Test
    @DisplayName("KTable should maintain latest value for each key")
    void testKTableUpdates() {
        StreamsBuilder builder = new StreamsBuilder();
        KTable<String, String> table = builder.table("input");
        table.toStream().to("output");
        
        testDriver = new TopologyTestDriver(builder.build(), props);
        TestInputTopic<String, String> input = testDriver.createInputTopic(
            "input", new StringSerializer(), new StringSerializer());
        TestOutputTopic<String, String> output = testDriver.createOutputTopic(
            "output", new StringDeserializer(), new StringDeserializer());
        
        // Send multiple updates for same key
        input.pipeInput("user1", "Alice");
        input.pipeInput("user1", "Alice Smith");
        input.pipeInput("user1", "Alice Smith Jr");
        
        // Should see all updates
        List<KeyValue<String, String>> results = output.readKeyValuesToList();
        assertEquals(3, results.size());
        assertEquals("Alice", results.get(0).value);
        assertEquals("Alice Smith", results.get(1).value);
        assertEquals("Alice Smith Jr", results.get(2).value);
    }
    
    @Test
    @DisplayName("KTable should handle deletions (null values)")
    void testKTableDeletion() {
        StreamsBuilder builder = new StreamsBuilder();
        KTable<String, String> table = builder.table("input");
        table.toStream().to("output");
        
        testDriver = new TopologyTestDriver(builder.build(), props);
        TestInputTopic<String, String> input = testDriver.createInputTopic(
            "input", new StringSerializer(), new StringSerializer());
        TestOutputTopic<String, String> output = testDriver.createOutputTopic(
            "output", new StringDeserializer(), new StringDeserializer());
        
        // Insert then delete
        input.pipeInput("user1", "Alice");
        input.pipeInput("user1", (String) null); // Deletion
        
        List<KeyValue<String, String>> results = output.readKeyValuesToList();
        assertEquals(2, results.size());
        assertEquals("Alice", results.get(0).value);
        assertNull(results.get(1).value); // Deletion
    }
    
    @Test
    @DisplayName("KTable filter should only emit matching updates")
    void testKTableFilter() {
        StreamsBuilder builder = new StreamsBuilder();
        KTable<String, String> table = builder.table("input");
        table.filter((k, v) -> v != null && v.contains("active"))
            .toStream()
            .to("output");
        
        testDriver = new TopologyTestDriver(builder.build(), props);
        TestInputTopic<String, String> input = testDriver.createInputTopic(
            "input", new StringSerializer(), new StringSerializer());
        TestOutputTopic<String, String> output = testDriver.createOutputTopic(
            "output", new StringDeserializer(), new StringDeserializer());
        
        input.pipeInput("user1", "status:active");
        input.pipeInput("user2", "status:inactive");
        input.pipeInput("user3", "status:active");
        
        List<String> results = output.readValuesToList();
        assertEquals(2, results.size());
        assertTrue(results.get(0).contains("active"));
        assertTrue(results.get(1).contains("active"));
    }
    
    @Test
    @DisplayName("KTable mapValues should transform values")
    void testKTableMapValues() {
        StreamsBuilder builder = new StreamsBuilder();
        KTable<String, String> table = builder.table("input");
        table.mapValues((readOnlyKey, value) -> value != null ? value.toUpperCase() : null)
            .toStream()
            .to("output");
        
        testDriver = new TopologyTestDriver(builder.build(), props);
        TestInputTopic<String, String> input = testDriver.createInputTopic(
            "input", new StringSerializer(), new StringSerializer());
        TestOutputTopic<String, String> output = testDriver.createOutputTopic(
            "output", new StringDeserializer(), new StringDeserializer());
        
        input.pipeInput("user1", "alice");
        input.pipeInput("user2", "bob");
        
        List<String> results = output.readValuesToList();
        assertEquals("ALICE", results.get(0));
        assertEquals("BOB", results.get(1));
    }
    
    @Test
    @DisplayName("KTable Demo topology should filter and enrich")
    void testKTableDemoTopology() {
        testDriver = new TopologyTestDriver(KTableDemo.createTopology().build(), props);
        
        TestInputTopic<String, String> input = testDriver.createInputTopic(
            KTableDemo.USER_UPDATES_TOPIC, 
            new StringSerializer(), 
            new StringSerializer());
        TestOutputTopic<String, String> output = testDriver.createOutputTopic(
            KTableDemo.USER_PROFILE_TOPIC, 
            new StringDeserializer(), 
            new StringDeserializer());
        
        // Active user - should pass through
        input.pipeInput("user1", "name:Alice,status:active");
        
        // Inactive user - should be filtered
        input.pipeInput("user2", "name:Bob,status:inactive");
        
        // Another active user
        input.pipeInput("user3", "name:Charlie,status:active");
        
        List<String> results = output.readValuesToList();
        assertEquals(2, results.size()); // Only active users
        
        // Verify enrichment (timestamp added)
        assertTrue(results.get(0).contains("updated:"));
        assertTrue(results.get(1).contains("updated:"));
    }
    
    @Test
    @DisplayName("GlobalKTable join should enrich stream")
    void testGlobalKTableJoin() {
        testDriver = new TopologyTestDriver(GlobalKTableDemo.createTopology().build(), props);
        
        TestInputTopic<String, String> productsInput = testDriver.createInputTopic(
            GlobalKTableDemo.PRODUCTS_TOPIC, 
            new StringSerializer(), 
            new StringSerializer());
        TestInputTopic<String, String> txnInput = testDriver.createInputTopic(
            GlobalKTableDemo.TRANSACTIONS_TOPIC, 
            new StringSerializer(), 
            new StringSerializer());
        TestOutputTopic<String, String> output = testDriver.createOutputTopic(
            GlobalKTableDemo.ENRICHED_TRANSACTIONS_TOPIC, 
            new StringDeserializer(), 
            new StringDeserializer());
        
        // First, populate product catalog
        productsInput.pipeInput("P001", "Laptop,price:$1200");
        productsInput.pipeInput("P002", "Mouse,price:$25");
        
        // Then send transactions
        txnInput.pipeInput("txn1", "T001:P001:2"); // Transaction for product P001
        txnInput.pipeInput("txn2", "T002:P002:5"); // Transaction for product P002
        
        List<String> results = output.readValuesToList();
        assertEquals(2, results.size());
        assertTrue(results.get(0).contains("Laptop"));
        assertTrue(results.get(1).contains("Mouse"));
    }
}
