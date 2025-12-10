package com.kafkatutorials.streams.windowing;

import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

class WindowingTest {
    private TopologyTestDriver testDriver;
    
    @AfterEach
    void tearDown() {
        if (testDriver != null) testDriver.close();
    }
    
    @Test
    void testTumblingWindow() {
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> input = builder.stream("input");
        
        input.groupByKey()
            .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(1)))
            .count()
            .toStream()
            .to("output");
        
        Properties props = new Properties();
        props.put("application.id", "test");
        props.put("bootstrap.servers", "dummy:1234");
        
        testDriver = new TopologyTestDriver(builder.build(), props);
        
        TestInputTopic<String, String> inputTopic = testDriver.createInputTopic(
            "input", new StringSerializer(), new StringSerializer());
        TestOutputTopic<?, ?> outputTopic = testDriver.createOutputTopic(
            "output", new StringDeserializer(), new StringDeserializer());
        
        Instant base = Instant.now();
        
        // Same window
        inputTopic.pipeInput("key1", "val1", base);
        inputTopic.pipeInput("key1", "val2", base.plusSeconds(30));
        
        // Different window
        inputTopic.pipeInput("key1", "val3", base.plusSeconds(70));
        
        // Should have 2 windows
        assertEquals(3, outputTopic.getQueueSize());
    }
}
