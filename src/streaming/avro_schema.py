import json
from io import StringIO
from typing import Any

from fastavro import json_reader, json_writer

from src.config.constants import AVSC_PATH


def load_pubsub_schema() -> dict:
    """
    Memuat skema Avro dari file .avsc lokal.
    """
    if not AVSC_PATH.exists():
        raise FileNotFoundError(
            f"File skema Avro tidak ditemukan pada jalur: {AVSC_PATH}"
        )

    with AVSC_PATH.open("r", encoding="utf-8") as file:
        return json.load(file)


def encode_avro_json(event: dict[str, Any], schema: dict[str, Any]) -> bytes:
    """
    Encode a dictionary event into Avro JSON bytes
    """
    buffer = StringIO()
    json_writer(buffer, schema, [event])
    return buffer.getvalue().encode("utf-8")


def decode_avro_json(element: bytes, schema: dict[str, Any]) -> dict[str, Any]:
    """
    Decode Avro JSON bytes back into a dictionary event
    """
    return next(json_reader(StringIO(element.decode("utf-8")), schema))
