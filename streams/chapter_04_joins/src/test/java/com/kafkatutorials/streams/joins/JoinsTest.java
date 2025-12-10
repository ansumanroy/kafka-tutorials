package com.kafkatutorials.streams.joins;

import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

class JoinsTest {
    
    private TopologyTestDriver testDriver;
    private Properties props;
    
    @BeforeEach
    void setup() {
        props = JoinConfigHelper.getBasicConfig("test", "dummy:1234");
    }
    
    @AfterEach
    void tearDown() {
        if (testDriver != null) {
            testDriver.close();
        }
    }
    
    @Test
    @DisplayName("Stream-stream inner join should match records within window")
    void testStreamStreamInnerJoin() {
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> left = builder.stream("left");
        KStream<String, String> right = builder.stream("right");
        
        left.join(right,
            (leftValue, rightValue) -> leftValue + "+" + rightValue,
            JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofMinutes(5))
        ).to("output");
        
        testDriver = new TopologyTestDriver(builder.build(), props);
        TestInputTopic<String, String> leftTopic = testDriver.createInputTopic(
            "left", new StringSerializer(), new StringSerializer());
        TestInputTopic<String, String> rightTopic = testDriver.createInputTopic(
            "right", new StringSerializer(), new StringSerializer());
        TestOutputTopic<String, String> output = testDriver.createOutputTopic(
            "output", new StringDeserializer(), new StringDeserializer());
        
        Instant now = Instant.now();
        
        // Send matching records
        leftTopic.pipeInput("key1", "L1", now);
        rightTopic.pipeInput("key1", "R1", now.plusSeconds(30));
        
        List<String> results = output.readValuesToList();
        assertEquals(1, results.size());
        assertEquals("L1+R1", results.get(0));
    }
    
    @Test
    @DisplayName("Stream-stream left join should keep all left records")
    void testStreamStreamLeftJoin() {
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> left = builder.stream("left");
        KStream<String, String> right = builder.stream("right");
        
        left.leftJoin(right,
            (leftValue, rightValue) -> leftValue + "+" + (rightValue != null ? rightValue : "NULL"),
            JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofMinutes(5))
        ).to("output");
        
        testDriver = new TopologyTestDriver(builder.build(), props);
        TestInputTopic<String, String> leftTopic = testDriver.createInputTopic(
            "left", new StringSerializer(), new StringSerializer());
        TestInputTopic<String, String> rightTopic = testDriver.createInputTopic(
            "right", new StringSerializer(), new StringSerializer());
        TestOutputTopic<String, String> output = testDriver.createOutputTopic(
            "output", new StringDeserializer(), new StringDeserializer());
        
        Instant now = Instant.now();
        
        // Left with match
        leftTopic.pipeInput("key1", "L1", now);
        rightTopic.pipeInput("key1", "R1", now.plusSeconds(10));
        
        // Left without match
        leftTopic.pipeInput("key2", "L2", now.plusSeconds(20));
        
        List<String> results = output.readValuesToList();
        assertEquals(2, results.size());
        assertEquals("L1+R1", results.get(0));
        assertEquals("L2+NULL", results.get(1));
    }
    
    @Test
    @DisplayName("Stream-table join should enrich stream with table data")
    void testStreamTableJoin() {
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> stream = builder.stream("stream");
        KTable<String, String> table = builder.table("table");
        
        stream.join(table,
            (streamValue, tableValue) -> streamValue + "+" + tableValue
        ).to("output");
        
        testDriver = new TopologyTestDriver(builder.build(), props);
        TestInputTopic<String, String> streamTopic = testDriver.createInputTopic(
            "stream", new StringSerializer(), new StringSerializer());
        TestInputTopic<String, String> tableTopic = testDriver.createInputTopic(
            "table", new StringSerializer(), new StringSerializer());
        TestOutputTopic<String, String> output = testDriver.createOutputTopic(
            "output", new StringDeserializer(), new StringDeserializer());
        
        // Populate table first
        tableTopic.pipeInput("user1", "Alice");
        tableTopic.pipeInput("user2", "Bob");
        
        // Stream events
        streamTopic.pipeInput("user1", "order1");
        streamTopic.pipeInput("user2", "order2");
        
        List<String> results = output.readValuesToList();
        assertEquals(2, results.size());
        assertTrue(results.get(0).contains("Alice"));
        assertTrue(results.get(1).contains("Bob"));
    }
    
    @Test
    @DisplayName("Table-table join should maintain current state")
    void testTableTableJoin() {
        StreamsBuilder builder = new StreamsBuilder();
        KTable<String, String> table1 = builder.table("table1");
        KTable<String, String> table2 = builder.table("table2");
        
        table1.join(table2,
            (value1, value2) -> value1 + "+" + value2
        ).toStream().to("output");
        
        testDriver = new TopologyTestDriver(builder.build(), props);
        TestInputTopic<String, String> topic1 = testDriver.createInputTopic(
            "table1", new StringSerializer(), new StringSerializer());
        TestInputTopic<String, String> topic2 = testDriver.createInputTopic(
            "table2", new StringSerializer(), new StringSerializer());
        TestOutputTopic<String, String> output = testDriver.createOutputTopic(
            "output", new StringDeserializer(), new StringDeserializer());
        
        // Initial state
        topic1.pipeInput("key1", "A");
        topic2.pipeInput("key1", "1");
        
        // Update table1
        topic1.pipeInput("key1", "B");
        
        List<String> results = output.readValuesToList();
        assertEquals(2, results.size());
        assertEquals("A+1", results.get(0));
        assertEquals("B+1", results.get(1)); // Updated join result
    }
}
