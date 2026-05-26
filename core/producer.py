#!/usr/bin/env python3
"""File watcher that publishes new .jsonl files to a Kafka topic.

Usage:
    python -m core.producer
"""

import json
import logging
import time
from pathlib import Path
from typing import override

from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from watchdog.events import (
    DirCreatedEvent,
    DirMovedEvent,
    FileCreatedEvent,
    FileMovedEvent,
    FileSystemEventHandler,
)
from watchdog.observers import Observer

from core.config import INCOMING_DIR, KAFKA_BOOTSTRAP, KAFKA_TOPIC

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)


def ensure_topic_exists(bootstrap_servers: str, topic: str) -> None:
    """Create the Kafka topic if it doesn't already exist."""
    try:
        admin_client = KafkaAdminClient(
            bootstrap_servers=bootstrap_servers, client_id="bgd-producer"
        )
        existing = admin_client.list_topics()
        if topic not in existing:
            new_topic = NewTopic(name=topic, num_partitions=1, replication_factor=1)
            admin_client.create_topics([new_topic])
            log.info("Created topic: %s", topic)
        else:
            log.info("Topic already exists: %s", topic)
        admin_client.close()
    except Exception as e:
        log.warning("Topic check/create warning: %s", e)


class JsonlHandler(FileSystemEventHandler):
    """Watch for new .jsonl files and publish to Kafka."""

    def __init__(self, producer: KafkaProducer, topic: str):
        self.producer: KafkaProducer = producer
        self.topic: str = topic
        self.seen: set[str] = set()

    @override
    def on_created(self, event: DirCreatedEvent | FileCreatedEvent) -> None:
        if event.is_directory:
            return
        filepath = Path(str(event.src_path))
        self._publish(filepath)

    @override
    def on_moved(self, event: DirMovedEvent | FileMovedEvent) -> None:
        """Handle files moved into the watched directory."""
        if event.is_directory:
            return
        dest = Path(str(event.dest_path))
        self._publish(dest)

    def _publish(self, filepath: Path) -> None:
        if filepath.suffix != ".jsonl":
            return
        if filepath.name in self.seen:
            return

        # Brief delay to let the file finish writing
        time.sleep(0.5)

        self.seen.add(filepath.name)
        message = {"filepath": str(filepath.resolve()), "filename": filepath.name}
        self.producer.send(self.topic, message)
        log.info("Published: %s", filepath.name)


def run_producer() -> None:
    watch_dir = INCOMING_DIR
    watch_dir.mkdir(parents=True, exist_ok=True)

    ensure_topic_exists(KAFKA_BOOTSTRAP, KAFKA_TOPIC)

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    handler = JsonlHandler(producer, KAFKA_TOPIC)

    # Publish any .jsonl files already sitting in incoming/
    for existing_file in sorted(watch_dir.glob("*.jsonl")):
        handler.seen.add(existing_file.name)
        message = {
            "filepath": str(existing_file.resolve()),
            "filename": existing_file.name,
        }
        producer.send(KAFKA_TOPIC, message)
        log.info("Published existing: %s", existing_file.name)

    observer = Observer()
    observer.schedule(handler, str(watch_dir), recursive=False)
    observer.start()

    log.info("Watching %s for new .jsonl files...", watch_dir)
    log.info("Publishing to Kafka topic: %s", KAFKA_TOPIC)

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        log.info("Stopping producer...")
        observer.stop()
    finally:
        observer.join()
        producer.close()


if __name__ == "__main__":
    run_producer()
