import argparse
import signal
import time
from io import StringIO
from typing import Any

from fastavro import json_writer
from google.api_core.exceptions import GoogleAPIError
from google.cloud import pubsub_v1

from src.config.constants import END_DATE, START_DATE
from src.config.settings import settings
from src.observability.logger import get_logger
from src.streaming.avro_schema import load_pubsub_schema
from src.streaming.event_generator import generate_event, load_profile

logger = get_logger(__name__)


class GracefulShutdown:
    """
    Handle OS signals to shut down the publisher loop gracefully.
    """

    def __init__(self) -> None:
        self.should_stop = False
        signal.signal(signal.SIGINT, self._handle)
        signal.signal(signal.SIGTERM, self._handle)

    def _handle(self, signum: int, _frame: Any) -> None:
        logger.info("Signal %s received. Stopping publisher loop.", signum)
        self.should_stop = True


def publish_event(
    publisher: pubsub_v1.PublisherClient,
    topic_path: str,
    event: dict[str, Any],
    schema: dict[str, Any],
) -> str:
    """
    Publish a single Avro-encoded event to Pub/Sub.
    """
    buffer = StringIO()
    json_writer(buffer, schema, [event])
    payload = buffer.getvalue().encode("utf-8")

    attributes = {
        "event_id": str(event["event_id"]),
        "event_time": str(event["event_time"]),
    }

    future = publisher.publish(topic_path, data=payload, **attributes)
    return future.result()


def run_publisher(rate: float, max_events: int | None = None) -> None:
    """
    Loop and publish events at a specified rate until stopped
    or max events reached.
    """
    profile = load_profile()
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(settings.gcp_project_id, settings.pubsub_topic)
    schema = load_pubsub_schema()

    shutdown = GracefulShutdown()
    interval = 1.0 / rate
    count = 0

    logger.info(
        "Starting publisher [rate=%s msg/s, max=%s, topic=%s]",
        rate,
        max_events or "unlimited",
        topic_path,
    )

    try:
        while not shutdown.should_stop:
            loop_start = time.monotonic()
            event = generate_event(profile, START_DATE, END_DATE)

            try:
                msg_id = publish_event(publisher, topic_path, event, schema)
                count += 1
                logger.info(
                    "Published message %s | msg_id=%s | event_id=%s",
                    count,
                    msg_id,
                    event["event_id"],
                )

            except GoogleAPIError as err:
                logger.error(
                    "Failed to publish event %s: %s",
                    event.get("event_id"),
                    err,
                )

            if max_events and count >= max_events:
                logger.info("Reached target max events (%s). Exiting.", max_events)
                break

            elapsed = time.monotonic() - loop_start
            time.sleep(max(0.0, interval - elapsed))

    finally:
        logger.info("Publisher stopped. Total published messages: %s", count)


def parse_args() -> argparse.Namespace:
    """
    Parse and validate command-line arguments for the publisher script.
    """
    parser = argparse.ArgumentParser(description="Google Pub/Sub Event Publisher")
    parser.add_argument("--rate", type=float, default=1.0)
    parser.add_argument("--max-events", type=int, default=None)

    args = parser.parse_args()

    if args.rate <= 0:
        parser.error("--rate must be > 0")

    if args.max_events is not None and args.max_events <= 0:
        parser.error("--max-events must be > 0")

    return args


if __name__ == "__main__":
    args = parse_args()
    run_publisher(rate=args.rate, max_events=args.max_events)
