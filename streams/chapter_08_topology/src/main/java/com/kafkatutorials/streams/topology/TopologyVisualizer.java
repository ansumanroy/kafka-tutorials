package com.kafkatutorials.streams.topology;

import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyDescription;

/**
 * Utility to visualize and analyze topology structure.
 */
public class TopologyVisualizer {
    
    /**
     * Generate a simple text visualization of the topology.
     */
    public static String visualize(Topology topology) {
        StringBuilder sb = new StringBuilder();
        TopologyDescription desc = topology.describe();
        
        sb.append("Topology Visualization\n");
        sb.append("=====================\n\n");
        
        desc.subtopologies().forEach(sub -> {
            sb.append("Sub-topology ").append(sub.id()).append(":\n");
            sub.nodes().forEach(node -> {
                sb.append("  [").append(node.name()).append("]\n");
                node.predecessors().forEach(pred -> 
                    sb.append("    <- ").append(pred.name()).append("\n")
                );
                node.successors().forEach(succ -> 
                    sb.append("    -> ").append(succ.name()).append("\n")
                );
            });
            sb.append("\n");
        });
        
        return sb.toString();
    }
    
    /**
     * Count repartition operations in topology.
     */
    public static int countRepartitions(Topology topology) {
        int count = 0;
        TopologyDescription desc = topology.describe();
        
        for (TopologyDescription.Subtopology sub : desc.subtopologies()) {
            for (TopologyDescription.Node node : sub.nodes()) {
                if (node.name().contains("KSTREAM-SINK") || 
                    node.name().contains("repartition")) {
                    count++;
                }
            }
        }
        
        return count;
    }
}
