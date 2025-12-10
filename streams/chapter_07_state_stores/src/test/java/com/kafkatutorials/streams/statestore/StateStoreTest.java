package com.kafkatutorials.streams.statestore;

import org.apache.kafka.common.serialization.*;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.junit.jupiter.api.*;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

class StateStoreTest {
    private TopologyTestDriver testDriver;
    
    @AfterEach
    void tearDown() {
        if (testDriver != null) testDriver.close();
    }
    
    @Test
    void testKeyValueStore() {
        testDriver = new TopologyTestDriver(KeyValueStoreDemo.createTopology().build(), getProps());
        
        TestInputTopic<String, String> input = testDriver.createInputTopic(
            "events", new StringSerializer(), new StringSerializer());
        
        input.pipeInput("key1", "event1");
        input.pipeInput("key1", "event2");
        
        // Query the state store
        KeyValueStore<String, Long> store = testDriver.getKeyValueStore("event-counts");
        assertEquals(2L, store.get("key1"));
    }
    
    private Properties getProps() {
        Properties props = new Properties();
        props.put("application.id", "test");
        props.put("bootstrap.servers", "dummy:1234");
        return props;
    }
}
