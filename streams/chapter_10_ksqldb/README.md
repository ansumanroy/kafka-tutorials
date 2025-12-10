# Chapter 10 - KSQL/ksqlDB Integration

KSQL is SQL for Kafka Streams - declarative stream processing without Java code.

## KSQL vs Kafka Streams

| Aspect | Kafka Streams | KSQL/ksqlDB |
|--------|---------------|-------------|
| **Language** | Java/Scala | SQL |
| **Learning Curve** | Steep | Easy |
| **Flexibility** | High | Limited |
| **Deployment** | Embedded | Server |
| **Use Case** | Complex logic | Simple transformations |

## Setup with Docker

### Start ksqlDB

```bash
# Start Kafka first
docker-compose -f infra/docker-compose-apache.yml up -d

# Start ksqlDB
docker-compose -f streams/chapter_10_ksqldb/docker-compose-ksqldb.yml up -d
```

### Access ksqlDB CLI

```bash
docker exec -it kafka-tutorials-ksqldb-cli ksql http://ksqldb-server:8088
```

## KSQL Basics

### Create Stream

```sql
CREATE STREAM page_views (
    view_id VARCHAR KEY,
    user_id VARCHAR,
    page_url VARCHAR,
    view_time BIGINT
) WITH (
    KAFKA_TOPIC='page-views',
    VALUE_FORMAT='JSON',
    PARTITIONS=1
);
```

### Create Table

```sql
CREATE TABLE users (
    user_id VARCHAR PRIMARY KEY,
    name VARCHAR,
    email VARCHAR
) WITH (
    KAFKA_TOPIC='users',
    VALUE_FORMAT='JSON'
);
```

### Queries

```sql
-- Push query (streaming)
SELECT * FROM page_views EMIT CHANGES;

-- Pull query (point-in-time)
SELECT * FROM users WHERE user_id = 'user123';

-- Aggregation
SELECT user_id, COUNT(*) as view_count
FROM page_views
GROUP BY user_id
EMIT CHANGES;
```

## Windowing in KSQL

```sql
-- Tumbling window
CREATE TABLE click_counts AS
    SELECT 
        element_id,
        COUNT(*) as clicks
    FROM click_events
    WINDOW TUMBLING (SIZE 1 MINUTE)
    GROUP BY element_id
    EMIT CHANGES;

-- Session window
CREATE TABLE user_sessions AS
    SELECT 
        user_id,
        COUNT(*) as events
    FROM page_views
    WINDOW SESSION (5 MINUTES)
    GROUP BY user_id
    EMIT CHANGES;
```

## Joins in KSQL

```sql
-- Stream-table join
CREATE STREAM enriched_orders AS
    SELECT 
        o.order_id,
        o.amount,
        u.name as customer_name
    FROM orders o
    LEFT JOIN users u ON o.user_id = u.user_id
    EMIT CHANGES;

-- Stream-stream join
CREATE STREAM matched_events AS
    SELECT *
    FROM stream1 s1
    INNER JOIN stream2 s2
        WITHIN 5 MINUTES
        ON s1.user_id = s2.user_id
    EMIT CHANGES;
```

## Java REST Client (used here)

This chapter uses a lightweight REST client (Apache HttpClient + Gson) to call ksqlDB endpoints directly:

- POST `/ksql` for DDL/DML (CREATE STREAM/TABLE, SHOW STREAMS/TABLES)
- POST `/query` for pull queries (point-in-time)
- POST `/query-stream` for push queries (streaming; we read a few rows)

See `KsqlDbClient.java` for examples:
```java
// Create client
KsqlDbClient client = new KsqlDbClient("http://localhost:8088");

// DDL
client.executeStatement("SHOW STREAMS;");

// Pull query
client.executePullQuery("SELECT * FROM test_stream EMIT CHANGES LIMIT 1;");

// Push query (read first 5 rows)
// client.executePushQuery("SELECT * FROM test_stream EMIT CHANGES;", 5);
```

## Running Examples

### 1. Start Services

```bash
docker-compose -f streams/chapter_10_ksqldb/docker-compose-ksqldb.yml up -d
```

### 2. Run SQL Scripts

```bash
# Copy scripts to container
docker cp streams/chapter_10_ksqldb/ksql-scripts/ kafka-tutorials-ksqldb-server:/opt/

# Execute in ksqlDB CLI
docker exec -it kafka-tutorials-ksqldb-cli ksql http://ksqldb-server:8088
ksql> RUN SCRIPT '/opt/ksql-scripts/01-create-streams.sql';
```

### 3. Run Java Client

```bash
cd streams/chapter_10_ksqldb
gradle runKsqlClient
```

## When to Use KSQL

### ✅ Use KSQL When:
- Simple transformations (filter, map, aggregate)
- SQL-familiar team
- Rapid prototyping
- Ad-hoc queries

### ⚠️ Use Kafka Streams When:
- Complex business logic
- Custom processors
- Need full control
- Advanced state management

## Key Features

- **SQL interface** for streams
- **Server-based** deployment
- **REST API** for integration
- **Interactive queries**
- **Exactly-once semantics**
- **Built on Kafka Streams** (under the hood)

## Production Considerations

1. **Resource planning**: ksqlDB server needs adequate CPU/memory
2. **HA setup**: Multiple ksqlDB servers for high availability
3. **Monitoring**: Track query performance, lag
4. **Security**: Enable authentication, SSL
5. **Backups**: Export DDL statements

## Key Takeaways

1. **KSQL = SQL for Kafka Streams**
2. **Push queries** = streaming results
3. **Pull queries** = point-in-time lookups
4. **Java client** for programmatic access
5. **Choose based on complexity** and team skills
6. **Built on Streams** = same guarantees

---

## Additional Resources

- [ksqlDB Documentation](https://docs.ksqldb.io/)
- [ksqlDB Examples](https://github.com/confluentinc/ksqldb/tree/master/docs/tutorials)
- [Java Client API](https://docs.ksqldb.io/en/latest/developer-guide/ksqldb-clients/java-client/)
