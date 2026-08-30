from pathlib import Path
from shutil import copy2

from setuptools import find_packages, setup
from setuptools.command.build_py import build_py


class BuildPyWithAvroSchema(build_py):
    def run(self) -> None:
        super().run()

        project_root = Path(__file__).resolve().parent
        source_schema = (
            project_root / "infra" / "terraform" / "schemas" / "trip_event.avsc"
        )
        target_schema = Path(self.build_lib) / "src" / "streaming" / "trip_event.avsc"

        if not source_schema.is_file():
            raise FileNotFoundError(f"Avro schema not found: {source_schema}")

        target_schema.parent.mkdir(parents=True, exist_ok=True)
        copy2(source_schema, target_schema)


setup(
    name="cloud-data-pipeline",
    version="0.1.0",
    packages=find_packages(include=["src", "src.*"]),
    package_data={"src.streaming": ["trip_event.avsc"]},
)
