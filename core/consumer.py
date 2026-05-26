#!/usr/bin/env python3
"""Kafka consumer that triggers the ETL pipeline on new file messages.

Usage:
    python -m core.consumer
"""

import json
import logging
import sys
from pathlib import Path

from kafka import KafkaConsumer

from core.config import KAFKA_BOOTSTRAP, KAFKA_GROUP_ID, KAFKA_TOPIC
from core.db import check_connection
from core.watcher import move_to_processed, run_pipeline_stages

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)


def run_consumer() -> None:
    log.info("Checking database connection...")
    if not check_connection():
        log.error("Cannot connect to database. Is PostgreSQL running?")
        sys.exit(1)
    log.info("Database connection OK")

    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="earliest",
        group_id=KAFKA_GROUP_ID,
    )

    log.info("Subscribed to topic: %s", KAFKA_TOPIC)
    log.info("Waiting for messages...")

    for message in consumer:
        data = message.value
        filepath = Path(data["filepath"])
        filename = data["filename"]

        log.info("Received message: %s", filename)

        if not filepath.exists():
            log.error("File not found: %s", filepath)
            continue

        try:
            run_pipeline_stages(filepath)
            move_to_processed(filepath)
        except Exception:
            log.exception("Pipeline failed for %s", filename)


if __name__ == "__main__":
    run_consumer()
