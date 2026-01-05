#!/usr/bin/env python3
"""
Restore messages from S3 backup to Kafka with exact offset preservation.

This script reads messages from S3, preserves offset metadata, and produces
them to Kafka topics. Since Kafka doesn't allow setting producer offsets directly,
messages are produced and offset mappings are stored for later consumer offset restoration.
"""

import json
import gzip
import boto3
import sys
import os
import argparse
from pathlib import Path
from typing import Dict, List, Tuple, Optional
from kafka import KafkaProducer
from kafka.errors import KafkaError
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def load_kafka_config() -> Dict:
    """Load Kafka configuration from environment variables."""
    config = {
        'bootstrap_servers': os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092'),
        'security_protocol': os.environ.get('KAFKA_SECURITY_PROTOCOL', 'PLAINTEXT'),
    }
    
    # Add SASL configuration if needed
    if config['security_protocol'] != 'PLAINTEXT':
        config['sasl_mechanism'] = os.environ.get('KAFKA_SASL_MECHANISM', '')
        if config['sasl_mechanism']:
            if 'SCRAM' in config['sasl_mechanism']:
                from kafka import SASLMechanism
                config['sasl_mechanism'] = SASLMechanism.SCRAM_SHA_512 if 'SHA_512' in config['sasl_mechanism'] else SASLMechanism.SCRAM_SHA_256
            else:
                from kafka import SASLMechanism
                config['sasl_mechanism'] = SASLMechanism.PLAIN
            
            username = os.environ.get('KAFKA_SASL_USERNAME', '')
            password = os.environ.get('KAFKA_SASL_PASSWORD', '')
            if username and password:
                from kafka import PlaintextAuth
                config['sasl_plain_username'] = username
                config['sasl_plain_password'] = password
    
    return config


def create_producer(config: Dict) -> KafkaProducer:
    """Create a Kafka producer with the given configuration."""
    producer_config = {
        'bootstrap_servers': config['bootstrap_servers'].split(','),
        'value_serializer': lambda v: json.dumps(v).encode('utf-8') if isinstance(v, dict) else v,
        'key_serializer': lambda k: k.encode('utf-8') if k and isinstance(k, str) else k,
        'acks': 'all',
        'retries': 3,
        'max_in_flight_requests_per_connection': 1,  # Ensure ordering
    }
    
    if config['security_protocol'] != 'PLAINTEXT':
        producer_config['security_protocol'] = config['security_protocol']
        if 'sasl_mechanism' in config:
            producer_config['sasl_mechanism'] = config['sasl_mechanism']
            producer_config['sasl_plain_username'] = config.get('sasl_plain_username', '')
            producer_config['sasl_plain_password'] = config.get('sasl_plain_password', '')
    
    return KafkaProducer(**producer_config)


def download_and_parse_messages(s3_client, bucket: str, prefix: str, topic: str, partition: int) -> List[Dict]:
    """Download and parse messages from S3 for a specific topic/partition."""
    messages = []
    
    s3_prefix = f"{prefix}/topics/{topic}/partition-{partition}/"
    logger.info(f"Downloading messages from s3://{bucket}/{s3_prefix}")
    
    try:
        paginator = s3_client.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=bucket, Prefix=s3_prefix)
        
        for page in pages:
            for obj in page.get('Contents', []):
                key = obj['Key']
                
                # Skip offset metadata files
                if 'offsets.json' in key:
                    continue
                
                # Download and parse message file
                try:
                    response = s3_client.get_object(Bucket=bucket, Key=key)
                    content = response['Body'].read()
                    
                    # Handle gzip compression
                    if key.endswith('.gz') or 'gzip' in response.get('ContentEncoding', ''):
                        content = gzip.decompress(content)
                    
                    # Parse JSON messages
                    try:
                        data = json.loads(content.decode('utf-8'))
                        # Handle both single message and array of messages
                        if isinstance(data, list):
                            messages.extend(data)
                        else:
                            messages.append(data)
                    except json.JSONDecodeError:
                        # Try parsing line by line (JSONL format)
                        for line in content.decode('utf-8').splitlines():
                            if line.strip():
                                try:
                                    messages.append(json.loads(line))
                                except json.JSONDecodeError:
                                    logger.warning(f"Failed to parse line in {key}: {line[:100]}")
                                    continue
                    
                    logger.info(f"Loaded {len(messages)} messages from {key}")
                except Exception as e:
                    logger.error(f"Error processing {key}: {e}")
                    continue
    
    except Exception as e:
        logger.error(f"Error downloading messages: {e}")
        return messages
    
    # Sort messages by offset to maintain order
    messages.sort(key=lambda m: (m.get('_partition', partition), m.get('_offset', 0)))
    
    return messages


def restore_messages(
    s3_bucket: str,
    backup_id: str,
    topic_name: Optional[str],
    output_dir: str
) -> Dict[str, List[Tuple[int, int, int]]]:
    """
    Restore messages from S3 to Kafka.
    
    Returns a mapping of topic -> list of (partition, original_offset, new_offset) tuples
    for later consumer offset restoration.
    """
    s3_client = boto3.client('s3')
    kafka_config = load_kafka_config()
    producer = create_producer(kafka_config)
    
    s3_prefix = f"backups/{backup_id}"
    offset_mappings = {}  # topic -> [(partition, old_offset, new_offset)]
    
    try:
        # List all topics in backup
        topics = set()
        if topic_name:
            topics.add(topic_name)
        else:
            # Discover topics from S3
            paginator = s3_client.get_paginator('list_objects_v2')
            pages = paginator.paginate(Bucket=s3_bucket, Prefix=f"{s3_prefix}/topics/", Delimiter='/')
            
            for page in pages:
                for prefix_info in page.get('CommonPrefixes', []):
                    topic = prefix_info['Prefix'].split('/')[-2]
                    topics.add(topic)
        
        logger.info(f"Restoring messages for topics: {', '.join(sorted(topics))}")
        
        for topic in sorted(topics):
            logger.info(f"Restoring topic: {topic}")
            offset_mappings[topic] = []
            
            # Discover partitions for this topic
            partitions = set()
            paginator = s3_client.get_paginator('list_objects_v2')
            pages = paginator.paginate(Bucket=s3_bucket, Prefix=f"{s3_prefix}/topics/{topic}/partition-", Delimiter='/')
            
            for page in pages:
                for prefix_info in page.get('CommonPrefixes', []):
                    partition_str = prefix_info['Prefix'].split('partition-')[-1].rstrip('/')
                    try:
                        partitions.add(int(partition_str))
                    except ValueError:
                        continue
            
            if not partitions:
                logger.warning(f"No partitions found for topic {topic}")
                continue
            
            logger.info(f"Found partitions for {topic}: {sorted(partitions)}")
            
            # Restore messages for each partition
            for partition in sorted(partitions):
                logger.info(f"Restoring partition {partition} of topic {topic}")
                
                messages = download_and_parse_messages(
                    s3_client, s3_bucket, s3_prefix, topic, partition
                )
                
                if not messages:
                    logger.warning(f"No messages found for {topic} partition {partition}")
                    continue
                
                logger.info(f"Producing {len(messages)} messages for {topic} partition {partition}")
                
                partition_offset_map = []
                first_new_offset = None
                
                for msg in messages:
                    # Extract message data
                    key = msg.get('key')
                    value = msg.get('value')
                    original_offset = msg.get('_offset', 0)
                    timestamp = msg.get('_timestamp', None)
                    
                    if value is None:
                        logger.warning(f"Skipping message with no value at offset {original_offset}")
                        continue
                    
                    # Produce message
                    future = producer.send(
                        topic,
                        key=key,
                        value=value,
                        partition=partition,
                        timestamp_ms=timestamp
                    )
                    
                    # Wait for result to get new offset
                    try:
                        record_metadata = future.get(timeout=30)
                        new_offset = record_metadata.offset
                        
                        if first_new_offset is None:
                            first_new_offset = new_offset
                        
                        partition_offset_map.append((partition, original_offset, new_offset))
                        
                        if len(partition_offset_map) % 100 == 0:
                            logger.info(f"Produced {len(partition_offset_map)} messages for {topic} partition {partition}")
                    
                    except KafkaError as e:
                        logger.error(f"Error producing message: {e}")
                        continue
                
                # Store offset mappings
                offset_mappings[topic].extend(partition_offset_map)
                logger.info(f"Completed partition {partition}: {len(partition_offset_map)} messages restored")
                
                # Calculate offset shift
                if partition_offset_map:
                    offset_shift = first_new_offset - partition_offset_map[0][1]
                    logger.info(f"Offset shift for {topic} partition {partition}: {offset_shift}")
            
            logger.info(f"Completed topic {topic}: {len(offset_mappings[topic])} messages restored")
        
        # Flush producer
        producer.flush()
        logger.info("All messages produced, flushing producer...")
        
        # Save offset mappings to file
        output_file = os.path.join(output_dir, 'offset-mappings.json')
        with open(output_file, 'w') as f:
            json.dump(offset_mappings, f, indent=2)
        
        logger.info(f"Offset mappings saved to {output_file}")
        
        return offset_mappings
    
    finally:
        producer.close()


def main():
    parser = argparse.ArgumentParser(description='Restore messages from S3 backup to Kafka')
    parser.add_argument('s3_bucket', help='S3 bucket name')
    parser.add_argument('backup_id', help='Backup ID (e.g., 20240101_120000)')
    parser.add_argument('--topic', help='Specific topic to restore (default: all topics)')
    parser.add_argument('--output-dir', default='/tmp', help='Directory for offset mapping output')
    
    args = parser.parse_args()
    
    os.makedirs(args.output_dir, exist_ok=True)
    
    try:
        offset_mappings = restore_messages(
            args.s3_bucket,
            args.backup_id,
            args.topic,
            args.output_dir
        )
        
        total_messages = sum(len(mappings) for mappings in offset_mappings.values())
        logger.info(f"═══════════════════════════════════════════════════════════")
        logger.info(f"Message Restoration Complete")
        logger.info(f"═══════════════════════════════════════════════════════════")
        logger.info(f"Topics restored: {len(offset_mappings)}")
        logger.info(f"Total messages: {total_messages}")
        logger.info(f"Offset mappings: {os.path.join(args.output_dir, 'offset-mappings.json')}")
        logger.info(f"")
        logger.info(f"Next step: Run restore-offsets.sh to restore consumer group offsets")
        
        return 0
    
    except Exception as e:
        logger.error(f"Restoration failed: {e}", exc_info=True)
        return 1


if __name__ == '__main__':
    sys.exit(main())

