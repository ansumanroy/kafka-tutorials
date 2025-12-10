package com.kafkatutorials.streams.aggregations;

import org.apache.kafka.common.serialization.*;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.KStream;
import org.junit.jupiter.api.*;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

class AggregationsTest {
    private TopologyTestDriver testDriver;
    
    @AfterEach
    void tearDown() {
        if (testDriver != null) testDriver.close();
    }
    
    @Test
    void testCount() {
        testDriver = new TopologyTestDriver(CountAggregation.createTopology().build(), getProps());
        
        TestInputTopic<String, String> input = testDriver.createInputTopic(
            "events", new StringSerializer(), new StringSerializer());
        TestOutputTopic<String, Long> output = testDriver.createOutputTopic(
            "event-count-results", new StringDeserializer(), new LongDeserializer());
        
        input.pipeInput("key1", "event1");
        input.pipeInput("key1", "event2");
        input.pipeInput("key2", "event3");
        
        assertEquals(1L, output.readKeyValue().value);
        assertEquals(2L, output.readKeyValue().value);
        assertEquals(1L, output.readKeyValue().value);
    }
    
    private Properties getProps() {
        Properties props = new Properties();
        props.put("application.id", "test");
        props.put("bootstrap.servers", "dummy:1234");
        return props;
    }
}
