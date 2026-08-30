import json
from importlib.resources import files
from pathlib import Path
from typing import Any


def load_pubsub_schema() -> dict[str, Any]:
    """Load the schema packaged for Dataflow, or the Terraform source locally."""
    packaged_schema = files("src.streaming").joinpath("trip_event.avsc")

    if packaged_schema.is_file():
        return json.loads(packaged_schema.read_text(encoding="utf-8"))

    project_root = Path(__file__).resolve().parents[2]
    source_schema = project_root / "infra" / "terraform" / "schemas" / "trip_event.avsc"

    if not source_schema.is_file():
        raise FileNotFoundError(
            "Avro schema was not found in either the installed package or "
            f"the repository source: {source_schema}"
        )

    return json.loads(source_schema.read_text(encoding="utf-8"))
