package com.kafkatutorials.streams.kstream;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Predicate;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * Helper class providing reusable transformation functions and predicates
 * for KStream operations.
 */
public class TransformationHelper {
    
    /**
     * Creates basic Kafka Streams configuration.
     */
    public static Properties getBasicConfig(String applicationId, String bootstrapServers) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 0L); // Disable caching for demos
        return props;
    }
    
    /**
     * Predicate to filter numeric strings.
     */
    public static Predicate<String, String> isNumeric() {
        return (key, value) -> value != null && value.matches("\\d+");
    }
    
    /**
     * Predicate to filter strings longer than a threshold.
     */
    public static Predicate<String, String> longerThan(int length) {
        return (key, value) -> value != null && value.length() > length;
    }
    
    /**
     * Predicate to filter strings containing specific substring.
     */
    public static Predicate<String, String> contains(String substring) {
        return (key, value) -> value != null && value.toLowerCase().contains(substring.toLowerCase());
    }
    
    /**
     * Predicate to filter by key prefix.
     */
    public static Predicate<String, String> keyStartsWith(String prefix) {
        return (key, value) -> key != null && key.startsWith(prefix);
    }
    
    /**
     * Transform value to uppercase.
     */
    public static String toUpperCase(String value) {
        return value != null ? value.toUpperCase() : null;
    }
    
    /**
     * Transform value to lowercase.
     */
    public static String toLowerCase(String value) {
        return value != null ? value.toLowerCase() : null;
    }
    
    /**
     * Split string into words.
     */
    public static List<String> splitIntoWords(String value) {
        if (value == null || value.trim().isEmpty()) {
            return Arrays.asList();
        }
        return Arrays.asList(value.trim().split("\\s+"));
    }
    
    /**
     * Parse integer from string, return 0 if invalid.
     */
    public static Integer parseIntSafe(String value) {
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            return 0;
        }
    }
    
    /**
     * Add prefix to value.
     */
    public static String addPrefix(String prefix, String value) {
        return prefix + (value != null ? value : "");
    }
    
    /**
     * Add suffix to value.
     */
    public static String addSuffix(String suffix, String value) {
        return (value != null ? value : "") + suffix;
    }
    
    /**
     * Sanitize string (remove special characters, keep alphanumeric and spaces).
     */
    public static String sanitize(String value) {
        if (value == null) return null;
        return value.replaceAll("[^a-zA-Z0-9\\s]", "");
    }
    
    /**
     * Truncate string to maximum length.
     */
    public static String truncate(String value, int maxLength) {
        if (value == null) return null;
        return value.length() > maxLength ? value.substring(0, maxLength) : value;
    }
}
