package com.kafkatutorials.streams.exactlyonce;

import org.apache.kafka.common.serialization.*;
import org.apache.kafka.streams.*;
import org.junit.jupiter.api.*;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Testing exactly-once semantics.
 * Note: TopologyTestDriver doesn't simulate failures, so EOS behavior
 * can't be fully tested in unit tests. Integration tests needed for that.
 */
class ExactlyOnceTest {
    private TopologyTestDriver testDriver;
    
    @AfterEach
    void tearDown() {
        if (testDriver != null) testDriver.close();
    }
    
    @Test
    @DisplayName("EOS topology should process messages correctly")
    void testEOSTopology() {
        Properties props = new Properties();
        props.put("application.id", "test");
        props.put("bootstrap.servers", "dummy:1234");
        
        testDriver = new TopologyTestDriver(EOSProducerDemo.createTopology().build(), props);
        
        TestInputTopic<String, String> input = testDriver.createInputTopic(
            "eos-input", new StringSerializer(), new StringSerializer());
        TestOutputTopic<String, Long> output = testDriver.createOutputTopic(
            "eos-output", new StringDeserializer(), new LongDeserializer());
        
        // Send test data
        input.pipeInput("key1", "value1");
        input.pipeInput("key1", "value2");
        input.pipeInput("key2", "value3");
        
        // Verify counts
        assertEquals(1L, output.readKeyValue().value);
        assertEquals(2L, output.readKeyValue().value);
        assertEquals(1L, output.readKeyValue().value);
    }
    
    @Test
    @DisplayName("Transactional topology should be atomic")
    void testTransactionalTopology() {
        Properties props = new Properties();
        props.put("application.id", "test");
        props.put("bootstrap.servers", "dummy:1234");
        
        testDriver = new TopologyTestDriver(TransactionalStreamsDemo.createTopology().build(), props);
        
        TestInputTopic<String, String> input = testDriver.createInputTopic(
            "orders", new StringSerializer(), new StringSerializer());
        TestOutputTopic<String, Long> output = testDriver.createOutputTopic(
            "order-counts", new StringDeserializer(), new LongDeserializer());
        
        input.pipeInput("order1", "data");
        input.pipeInput("order1", "data");
        
        // Should see incremental counts
        assertEquals(1L, output.readKeyValue().value);
        assertEquals(2L, output.readKeyValue().value);
    }
}
