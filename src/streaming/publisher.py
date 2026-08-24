import argparse
import signal
import time
from typing import Any

from google.api_core.exceptions import GoogleAPIError
from google.cloud import pubsub_v1

from src.config.constants import END_DATE, START_DATE
from src.config.settings import GCP_PROJECT_ID, PUBSUB_TOPIC
from src.observability.logger import get_logger
from src.streaming.avro_schema import encode_avro_json, load_pubsub_schema
from src.streaming.event_generator import generate_event, load_profile

logger = get_logger(__name__)


class GracefulShutdown:
    """
    Handle OS signals to shut down the publisher loop gracefully
    """

    def __init__(self) -> None:
        self.should_stop = False
        signal.signal(signal.SIGINT, self._handle)
        signal.signal(signal.SIGTERM, self._handle)

    def _handle(self, signum: int, _frame: Any) -> None:
        logger.info(f"Signal {signum} received. Stopping publisher loop.")
        self.should_stop = True


def publish_event(
    publisher: pubsub_v1.PublisherClient,
    topic_path: str,
    event: dict[str, Any],
    schema: dict[str, Any],
) -> str:
    """
    Publish a single Avro-encoded event to Pub/Sub
    """
    payload = encode_avro_json(event, schema)

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
    topic_path = publisher.topic_path(GCP_PROJECT_ID, PUBSUB_TOPIC)
    schema = load_pubsub_schema()

    shutdown = GracefulShutdown()
    interval = 1.0 / rate
    count = 0

    logger.info(
        f"Starting publisher [rate={rate} msg/s, "
        f"max={max_events or 'unlimited'}, topic={topic_path}]"
    )

    try:
        while not shutdown.should_stop:
            loop_start = time.monotonic()
            event = generate_event(profile, START_DATE, END_DATE)

            try:
                msg_id = publish_event(publisher, topic_path, event, schema)
                count += 1
                logger.info(
                    f"Published message {count} | msg_id={msg_id} | "
                    f"event_id={event['event_id']}"
                )

            except GoogleAPIError as err:
                logger.error(f"Failed to publish event {event.get('event_id')}: {err}")

            if max_events and count >= max_events:
                logger.info(f"Reached target max events ({max_events}). Exiting.")
                break

            elapsed = time.monotonic() - loop_start
            time.sleep(max(0.0, interval - elapsed))

    finally:
        logger.info(f"Publisher stopped. Total published messages: {count}")


def parse_args() -> argparse.Namespace:
    """
    Parse and validate command-line arguments for the publisher script
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
