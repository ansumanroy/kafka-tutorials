package com.kafkatutorials.serialization;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import io.confluent.kafka.serializers.KafkaJsonSerializer;
import io.confluent.kafka.serializers.KafkaJsonDeserializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.*;

import java.util.Properties;

/**
 * Helper class for creating Kafka producer/consumer configurations
 * with different serialization formats.
 */
public class SerializationHelper {
    
    private static final String DEFAULT_BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String DEFAULT_SCHEMA_REGISTRY_URL = "http://localhost:8081";
    
    /**
     * Get base producer configuration.
     */
    public static Properties getBaseProducerConfig() {
        String bootstrapServers = System.getProperty("KAFKA_BOOTSTRAP_SERVERS",
                                   System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", DEFAULT_BOOTSTRAP_SERVERS));
        
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "serialization-demo");
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        
        return props;
    }
    
    /**
     * Get base consumer configuration.
     */
    public static Properties getBaseConsumerConfig() {
        String bootstrapServers = System.getProperty("KAFKA_BOOTSTRAP_SERVERS",
                                   System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", DEFAULT_BOOTSTRAP_SERVERS));
        
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "serialization-demo-group");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        
        return props;
    }
    
    /**
     * Producer config for String serialization.
     */
    public static Properties getStringProducerConfig() {
        Properties props = getBaseProducerConfig();
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return props;
    }
    
    /**
     * Consumer config for String deserialization.
     */
    public static Properties getStringConsumerConfig() {
        Properties props = getBaseConsumerConfig();
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        return props;
    }
    
    /**
     * Producer config for JSON serialization (using Confluent's KafkaJsonSerializer).
     */
    public static Properties getJsonProducerConfig() {
        Properties props = getBaseProducerConfig();
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaJsonSerializer.class.getName());
        return props;
    }
    
    /**
     * Consumer config for JSON deserialization.
     */
    public static Properties getJsonConsumerConfig() {
        Properties props = getBaseConsumerConfig();
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaJsonDeserializer.class.getName());
        props.put("json.value.type", UserEvent.class.getName());
        return props;
    }
    
    /**
     * Producer config for Avro serialization with Schema Registry.
     */
    public static Properties getAvroProducerConfig() {
        String schemaRegistryUrl = System.getProperty("SCHEMA_REGISTRY_URL",
                                     System.getenv().getOrDefault("SCHEMA_REGISTRY_URL", DEFAULT_SCHEMA_REGISTRY_URL));
        
        Properties props = getBaseProducerConfig();
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        props.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        // Auto register schemas
        props.put("auto.register.schemas", "true");
        return props;
    }
    
    /**
     * Consumer config for Avro deserialization with Schema Registry.
     */
    public static Properties getAvroConsumerConfig() {
        String schemaRegistryUrl = System.getProperty("SCHEMA_REGISTRY_URL",
                                     System.getenv().getOrDefault("SCHEMA_REGISTRY_URL", DEFAULT_SCHEMA_REGISTRY_URL));
        
        Properties props = getBaseConsumerConfig();
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
        props.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        // Use specific Avro record
        props.put("specific.avro.reader", "true");
        return props;
    }
    
    /**
     * Get Schema Registry URL from environment.
     */
    public static String getSchemaRegistryUrl() {
        return System.getProperty("SCHEMA_REGISTRY_URL",
                System.getenv().getOrDefault("SCHEMA_REGISTRY_URL", DEFAULT_SCHEMA_REGISTRY_URL));
    }
    
    /**
     * Print configuration summary.
     */
    public static void printConfig(Properties props, String configName) {
        System.out.println("\n=== " + configName + " Configuration ===");
        System.out.println("Bootstrap Servers: " + props.getProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG));
        System.out.println("Key Serializer:    " + props.getProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "N/A"));
        System.out.println("Value Serializer:  " + props.getProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "N/A"));
        
        if (props.containsKey(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG)) {
            System.out.println("Schema Registry:   " + props.getProperty(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG));
        }
        
        System.out.println();
    }
}
