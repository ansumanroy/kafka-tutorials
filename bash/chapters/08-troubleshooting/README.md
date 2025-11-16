# Chapter 08 - Troubleshooting & Debugging

Things go wrong: connections fail, topics are misconfigured, or consumers fall behind.

This chapter gives you a few CLI tools to inspect the health of your topics and consumer groups.

## Scripts

### Check topic health

```bash
bash/chapters/08-troubleshooting/check_topic_health.sh my-first-topic
```

This uses `kafka-topics.sh --describe` to show:

- Partitions and leaders.
- Replicas and in-sync replicas.
- Log end offsets.

Look out for:

- Partitions without a leader.
- Replicas that are not in sync.

### Check consumer lag

```bash
bash/chapters/08-troubleshooting/check_consumer_lag.sh my-group
```

This uses `kafka-consumer-groups.sh --describe` to show current offsets and lag per partition.

High lag means your consumers are not keeping up with producers.

## Common Issues

- **Connection errors**: Check `KAFKA_BOOTSTRAP_SERVERS`, network access, and security settings.
- **Auth errors**: For MSK, verify IAM/SASL configuration and client properties.
- **Topic not found**: Make sure you created the topic in Chapter 02 and are using the same name.
