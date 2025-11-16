# Chapter 07 - Serialization Basics

Kafka treats message values as **byte arrays**. It doesn't care whether your data is text, JSON, Avro, Protobuf, etc.

This chapter uses simple **JSON** messages to show that serialization is mainly a contract between producers and consumers.

## Scripts

### Send JSON messages

```bash
bash/chapters/07-serialization/send_json_messages.sh my-json-topic 5
```

This sends messages like:

```json
{"id": 1, "message": "hello-1"}
```

### Consume and pretty-print JSON

```bash
bash/chapters/07-serialization/consume_json_messages.sh my-json-topic
```

If `jq` is installed, messages are pretty-printed as JSON. Otherwise, they are printed as raw text.

## Concept Recap

- Kafka is **schema-agnostic**; it simply stores bytes.
- Producers and consumers must agree on how to **encode and decode** those bytes.
- In real systems, you might use formats like Avro, Protobuf, or JSON Schema with a schema registry. Later Python/Java examples can build on this.
