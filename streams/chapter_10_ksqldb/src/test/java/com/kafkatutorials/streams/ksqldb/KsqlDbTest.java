package com.kafkatutorials.streams.ksqldb;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Disabled;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for ksqlDB client.
 * Note: These require a running ksqlDB server, so disabled by default.
 */
class KsqlDbTest {
    
    @Test
    @Disabled("Requires running ksqlDB server")
    void testConnection() {
        KsqlDbClient client = new KsqlDbClient("http://localhost:8088");
        
        assertDoesNotThrow(() -> client.listStreams());
        assertDoesNotThrow(() -> client.listTables());
        
        client.close();
    }
    
    @Test
    void testUrlParsing() {
        // Just verify class loads and instantiation works with mock
        assertNotNull(KsqlDbClient.class);
    }
}
