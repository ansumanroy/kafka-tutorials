import os
import sys
from typing import Dict

from kafka import KafkaAdminClient


def load_env() -> Dict[str, str]:
    """Load Kafka connection details from env and validate required fields.

    This mirrors the Bash env loader logic at a high level.
    """
    bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
    if not bootstrap:
        print(
            "[error] KAFKA_BOOTSTRAP_SERVERS is not set. "
            "Copy infra/env-example.msk or infra/env-example.local and source it before running.",
            file=sys.stderr,
        )
        sys.exit(1)

    security_protocol = os.getenv("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT")
    sasl_mechanism = os.getenv("KAFKA_SASL_MECHANISM")
    sasl_username = os.getenv("KAFKA_SASL_USERNAME")
    sasl_password = os.getenv("KAFKA_SASL_PASSWORD")

    return {
        "bootstrap_servers": bootstrap.split(","),
        "security_protocol": security_protocol,
        "sasl_mechanism": sasl_mechanism,
        "sasl_plain_username": sasl_username,
        "sasl_plain_password": sasl_password,
    }


def check_connection() -> None:
    config = load_env()

    print(f"[info] Checking connectivity to Kafka at {','.join(config['bootstrap_servers'])}...")

    # Filter out empty auth values so local PLAINTEXT works without extra config.
    admin_kwargs = {k: v for k, v in config.items() if v}

    try:
        client = KafkaAdminClient(**admin_kwargs, client_id="kafka-tutorials-python-check")
        topics = client.list_topics()
        print(f"[info] Successfully connected. Found {len(topics)} topics.")
    except Exception as exc:  # pragma: no cover - simple demo script
        print("[error] Failed to connect to Kafka:", file=sys.stderr)
        print(f"        {exc}", file=sys.stderr)
        sys.exit(1)
    finally:
        try:
            client.close()  # type: ignore[has-type]
        except Exception:
            pass


if __name__ == "__main__":
    check_connection()
