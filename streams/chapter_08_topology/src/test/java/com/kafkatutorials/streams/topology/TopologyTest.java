package com.kafkatutorials.streams.topology;

import org.apache.kafka.common.serialization.*;
import org.apache.kafka.streams.*;
import org.junit.jupiter.api.*;

import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests using TopologyTestDriver - fast, deterministic testing.
 */
class TopologyTest {
    private TopologyTestDriver testDriver;
    
    @AfterEach
    void tearDown() {
        if (testDriver != null) testDriver.close();
    }
    
    @Test
    @DisplayName("Topology should filter and route correctly")
    void testTopologyRouting() {
        Topology topology = TopologyBuilder.buildComplexTopology();
        
        Properties props = new Properties();
        props.put("application.id", "test");
        props.put("bootstrap.servers", "dummy:1234");
        
        testDriver = new TopologyTestDriver(topology, props);
        
        TestInputTopic<String, String> input = testDriver.createInputTopic(
            "raw-events", new StringSerializer(), new StringSerializer());
        TestOutputTopic<String, String> errors = testDriver.createOutputTopic(
            "errors", new StringDeserializer(), new StringDeserializer());
        TestOutputTopic<String, String> info = testDriver.createOutputTopic(
            "info", new StringDeserializer(), new StringDeserializer());
        
        // Send test data
        input.pipeInput("1", "ERROR: Something went wrong");
        input.pipeInput("2", "INFO: All good");
        input.pipeInput("3", null);  // Should be filtered
        input.pipeInput("4", "");    // Should be filtered
        
        // Verify routing
        List<String> errorMsgs = errors.readValuesToList();
        List<String> infoMsgs = info.readValuesToList();
        
        assertEquals(1, errorMsgs.size());
        assertEquals(1, infoMsgs.size());
        assertTrue(errorMsgs.get(0).startsWith("ERROR"));
        assertTrue(infoMsgs.get(0).startsWith("INFO"));
    }
    
    @Test
    @DisplayName("Topology description should be inspectable")
    void testTopologyDescription() {
        Topology topology = TopologyBuilder.buildComplexTopology();
        
        assertNotNull(topology.describe());
        assertTrue(topology.describe().subtopologies().size() > 0);
    }
    
    @Test
    @DisplayName("Topology visualization should work")
    void testVisualization() {
        Topology topology = TopologyBuilder.buildComplexTopology();
        String viz = TopologyVisualizer.visualize(topology);
        
        assertNotNull(viz);
        assertTrue(viz.contains("Sub-topology"));
    }
}
