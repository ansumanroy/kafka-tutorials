import java.util.Properties;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListTopicsResult;

public class CheckConnection {
    public static void main(String[] args) throws Exception {
        String bootstrap = System.getenv("KAFKA_BOOTSTRAP_SERVERS");
        if (bootstrap == null || bootstrap.isEmpty()) {
            System.err.println("[error] KAFKA_BOOTSTRAP_SERVERS is not set. " +
                    "Copy infra/env-example.msk or infra/env-example.local and source it before running.");
            System.exit(1);
        }

        String securityProtocol = getenvOrDefault("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT");
        String saslMechanism = getenvOrDefault("KAFKA_SASL_MECHANISM", "");
        String saslUsername = getenvOrDefault("KAFKA_SASL_USERNAME", "");
        String saslPassword = getenvOrDefault("KAFKA_SASL_PASSWORD", "");

        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrap);
        props.put("client.id", "kafka-tutorials-java-check");
        props.put("security.protocol", securityProtocol);

        if (!saslMechanism.isEmpty()) {
            props.put("sasl.mechanism", saslMechanism);
        }
        if (!saslUsername.isEmpty() && !saslPassword.isEmpty()) {
            props.put("sasl.jaas.config", String.format(
                    "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"%s\" password=\"%s\";",
                    saslUsername,
                    saslPassword
            ));
        }

        System.out.println("[info] Checking connectivity to Kafka at " + bootstrap + "...");

        try (AdminClient admin = AdminClient.create(props)) {
            ListTopicsResult result = admin.listTopics();
            int count = result.names().get().size();
            System.out.println("[info] Successfully connected. Found " + count + " topics.");
        } catch (Exception e) {
            System.err.println("[error] Failed to connect to Kafka:");
            System.err.println("        " + e.getMessage());
            System.exit(1);
        }
    }

    private static String getenvOrDefault(String key, String defaultValue) {
        String value = System.getenv(key);
        return value != null ? value : defaultValue;
    }
}
