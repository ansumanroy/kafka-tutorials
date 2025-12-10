package com.kafkatutorials.streams.ksqldb;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;

/**
 * Lightweight ksqlDB REST client using plain HTTP.
 * Avoids heavy ksqlDB Java client dependencies that may be unavailable.
 *
 * Endpoints:
 *   - POST /ksql         : DDL/DML (CREATE STREAM/TABLE, SHOW STREAMS, etc.)
 *   - POST /query        : Pull queries (point-in-time)
 *   - POST /query-stream : Push queries (streaming; we read first N rows)
 */
public class KsqlDbClient {
    private static final Logger logger = LoggerFactory.getLogger(KsqlDbClient.class);

    private final String baseUrl; // e.g., http://localhost:8088
    private final CloseableHttpClient httpClient;
    private final Gson gson;

    public KsqlDbClient(String ksqldbUrl) {
        this.baseUrl = ksqldbUrl.endsWith("/") ? ksqldbUrl.substring(0, ksqldbUrl.length() - 1) : ksqldbUrl;
        this.httpClient = HttpClients.createDefault();
        this.gson = new Gson();
        logger.info("Using ksqlDB REST endpoint at {}", this.baseUrl);
    }

    /**
     * Execute a SQL statement (DDL/DML) via /ksql endpoint.
     */
    public void executeStatement(String sql) {
        JsonObject body = new JsonObject();
        body.addProperty("ksql", sql);
        body.add("streamsProperties", new JsonObject());
        postAndLog("/ksql", body.toString());
    }

    /**
     * Execute a pull query (point-in-time) via /query endpoint.
     */
    public void executePullQuery(String query) {
        JsonObject body = new JsonObject();
        body.addProperty("sql", query);
        JsonObject props = new JsonObject();
        body.add("properties", props);
        String response = post("/query", body.toString());
        if (response != null) {
            logger.info("Pull query response: {}", response);
        }
    }

    /**
     * Execute a push query (streaming) via /query-stream endpoint.
     * Reads up to 'limit' rows then stops.
     */
    public void executePushQuery(String query, int limit) {
        JsonObject body = new JsonObject();
        body.addProperty("sql", query);
        JsonObject props = new JsonObject();
        props.addProperty("auto.offset.reset", "earliest");
        body.add("properties", props);

        HttpPost post = new HttpPost(baseUrl + "/query-stream");
        post.setEntity(new StringEntity(body.toString(), ContentType.APPLICATION_JSON));
        try (CloseableHttpResponse response = httpClient.execute(post)) {
            if (response.getStatusLine().getStatusCode() >= 400) {
                logger.error("Push query failed: {}", response.getStatusLine());
                return;
            }
            HttpEntity entity = response.getEntity();
            if (entity == null) {
                logger.warn("Push query returned empty response");
                return;
            }
            try (BufferedReader reader = new BufferedReader(
                    new InputStreamReader(entity.getContent(), StandardCharsets.UTF_8))) {
                String line;
                int count = 0;
                while ((line = reader.readLine()) != null && count < limit) {
                    // Each line is JSON; log raw line for simplicity
                    logger.info("Push row: {}", line);
                    count++;
                }
            }
        } catch (IOException e) {
            logger.error("Failed to execute push query", e);
        }
    }

    /**
     * List streams via SHOW STREAMS
     */
    public void listStreams() {
        executeStatement("SHOW STREAMS;");
    }

    /**
     * List tables via SHOW TABLES
     */
    public void listTables() {
        executeStatement("SHOW TABLES;");
    }

    public void close() {
        try {
            httpClient.close();
        } catch (IOException e) {
            logger.warn("Error closing HTTP client", e);
        }
        logger.info("Closed ksqlDB REST client");
    }

    private String post(String path, String jsonBody) {
        HttpPost post = new HttpPost(baseUrl + path);
        post.setEntity(new StringEntity(jsonBody, ContentType.APPLICATION_JSON));
        try (CloseableHttpResponse response = httpClient.execute(post)) {
            int status = response.getStatusLine().getStatusCode();
            HttpEntity entity = response.getEntity();
            String payload = null;
            if (entity != null) {
                try (BufferedReader reader = new BufferedReader(
                        new InputStreamReader(entity.getContent(), StandardCharsets.UTF_8))) {
                    StringBuilder sb = new StringBuilder();
                    String line;
                    while ((line = reader.readLine()) != null) {
                        sb.append(line);
                    }
                    payload = sb.toString();
                }
            }
            if (status >= 400) {
                logger.error("Request to {} failed with status {} and body {}", path, status, payload);
                return null;
            }
            return payload;
        } catch (IOException e) {
            logger.error("HTTP request to {} failed", path, e);
            return null;
        }
    }

    private void postAndLog(String path, String jsonBody) {
        String response = post(path, jsonBody);
        if (response != null) {
            logger.info("Response from {}: {}", path, response);
        }
    }

    public static void main(String[] args) {
        String ksqldbUrl = System.getenv("KSQLDB_URL") != null
            ? System.getenv("KSQLDB_URL")
            : "http://localhost:8088";

        KsqlDbClient ksqlClient = new KsqlDbClient(ksqldbUrl);

        try {
            // List existing streams and tables
            ksqlClient.listStreams();
            ksqlClient.listTables();

            // Example: Create a stream
            ksqlClient.executeStatement(
                "CREATE STREAM test_stream (id VARCHAR KEY, value VARCHAR) " +
                "WITH (kafka_topic='test-topic', value_format='JSON', partitions=1);"
            );

            // Example: Pull query
            ksqlClient.executePullQuery("SELECT * FROM test_stream EMIT CHANGES LIMIT 1;");

            // Example: Push query (read first 5 rows)
            // ksqlClient.executePushQuery("SELECT * FROM test_stream EMIT CHANGES;", 5);

        } finally {
            ksqlClient.close();
        }
    }
}
