package com.kafkatutorials.streams.aggregations;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;

/**
 * Query materialized view (state store) from outside the topology.
 */
public class MaterializedViewDemo {
    
    public static Long queryCount(KafkaStreams streams, String key) {
        ReadOnlyKeyValueStore<String, Long> store = streams.store(
            StoreQueryParameters.fromNameAndType(
                "event-counts",
                QueryableStoreTypes.keyValueStore()
            )
        );
        
        return store.get(key);
    }
}
