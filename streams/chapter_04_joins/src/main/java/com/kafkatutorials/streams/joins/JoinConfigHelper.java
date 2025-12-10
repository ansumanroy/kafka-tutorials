package com.kafkatutorials.streams.joins;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.JoinWindows;

import java.time.Duration;
import java.util.Properties;

/**
 * Helper class for join configurations.
 */
public class JoinConfigHelper {
    
    public static Properties getBasicConfig(String applicationId, String bootstrapServers) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 0L);
        return props;
    }
    
    /**
     * Creates join window with specified duration.
     * Records join if within this time window of each other.
     */
    public static JoinWindows getJoinWindow(Duration duration) {
        return JoinWindows.ofTimeDifferenceWithNoGrace(duration);
    }
    
    /**
     * Creates join window with grace period.
     * Grace period allows late-arriving records.
     */
    public static JoinWindows getJoinWindowWithGrace(Duration duration, Duration grace) {
        return JoinWindows.ofTimeDifferenceAndGrace(duration, grace);
    }
    
    /**
     * Standard 5-minute join window.
     */
    public static JoinWindows getStandardWindow() {
        return JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofMinutes(5));
    }
}
