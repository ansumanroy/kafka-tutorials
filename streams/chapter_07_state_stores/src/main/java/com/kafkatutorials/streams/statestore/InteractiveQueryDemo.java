package com.kafkatutorials.streams.statestore;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;

import java.util.HashMap;
import java.util.Map;

/**
 * Interactive Queries - query state stores from outside the topology.
 * Useful for REST APIs, dashboards, health checks.
 */
public class InteractiveQueryDemo {
    
    /**
     * Query single key from store.
     */
    public static Long queryKey(KafkaStreams streams, String storeName, String key) {
        ReadOnlyKeyValueStore<String, Long> store = streams.store(
            StoreQueryParameters.fromNameAndType(
                storeName,
                QueryableStoreTypes.keyValueStore()
            )
        );
        
        return store.get(key);
    }
    
    /**
     * Query all keys in a range.
     */
    public static Map<String, Long> queryRange(
        KafkaStreams streams, 
        String storeName, 
        String from, 
        String to
    ) {
        ReadOnlyKeyValueStore<String, Long> store = streams.store(
            StoreQueryParameters.fromNameAndType(
                storeName,
                QueryableStoreTypes.keyValueStore()
            )
        );
        
        Map<String, Long> result = new HashMap<>();
        try (KeyValueIterator<String, Long> iterator = store.range(from, to)) {
            while (iterator.hasNext()) {
                var kv = iterator.next();
                result.put(kv.key, kv.value);
            }
        }
        
        return result;
    }
    
    /**
     * Query approximate count of entries.
     */
    public static long approximateCount(KafkaStreams streams, String storeName) {
        ReadOnlyKeyValueStore<String, Long> store = streams.store(
            StoreQueryParameters.fromNameAndType(
                storeName,
                QueryableStoreTypes.keyValueStore()
            )
        );
        
        return store.approximateNumEntries();
    }
}
