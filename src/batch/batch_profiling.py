import json
import math
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from src.config.constants import (
    BATCH_PROFILING,
    MAX_DISTANCE_POOL_SIZE,
    PROFILED_COLUMNS,
    RANDOM_SEED,
    RAW_DATA_DIR,
)


def load_dataset(raw_dir: Path) -> pd.DataFrame:
    """
    Load and concatenate Parquet files from the raw data directory
    """
    raw_files = list(raw_dir.glob("*.parquet"))

    if not raw_files:
        raise FileNotFoundError(f"No Parquet files found in {raw_dir}")

    return pd.concat([pd.read_parquet(file) for file in raw_files], ignore_index=True)


def analyze_hour_distribution(df: pd.DataFrame) -> dict[str, float]:
    """
    Calculate pickup-hour probabilities from batch data
    """
    pickup_datetime = pd.to_datetime(df["lpep_pickup_datetime"], errors="coerce")
    hour_counts = pickup_datetime.dt.hour.value_counts(normalize=True).sort_index()

    return {str(hour): float(hour_counts.get(hour, 0.0)) for hour in range(24)}


def build_reference_values(
    df: pd.DataFrame, column: str, max_size: int, seed: int
) -> list[float]:
    """
    Build a reference sample of numeric values
    """
    values = df[column].dropna().to_numpy()
    rng = np.random.default_rng(seed)

    if len(values) > max_size:
        return rng.choice(values, size=max_size, replace=False).tolist()

    return values.tolist()


def analyze_fare_per_mile(df: pd.DataFrame) -> dict[str, float]:
    """
    Calculate fare-per-mile percentiles from valid trips
    """
    valid_mask = (df["trip_distance"] > 0) & (df["fare_amount"] > 0)

    fare_per_mile = (
        df.loc[valid_mask, "fare_amount"] / df.loc[valid_mask, "trip_distance"]
    )
    fare_per_mile = fare_per_mile.replace([np.inf, -np.inf], np.nan).dropna()

    return {
        "p10": float(np.percentile(fare_per_mile, 10)),
        "median": float(np.median(fare_per_mile)),
        "p90": float(np.percentile(fare_per_mile, 90)),
    }


def analyze_tip_per_fare(df: pd.DataFrame) -> dict[str, float]:
    """
    Calculate tip-to-fare ratio percentiles from valid trips
    """
    valid_mask = (df["fare_amount"] > 0) & (df["tip_amount"] > 0)

    tip_per_fare = df.loc[valid_mask, "tip_amount"] / df.loc[valid_mask, "fare_amount"]
    tip_per_fare = tip_per_fare.replace([np.inf, -np.inf], np.nan).dropna()

    return {
        "p10": float(np.percentile(tip_per_fare, 10)),
        "median": float(np.median(tip_per_fare)),
        "p90": float(np.percentile(tip_per_fare, 90)),
    }


def analyze_speed_distribution(df: pd.DataFrame) -> dict[str, float]:
    """
    Calculate trip-speed percentiles from valid trips
    """
    pickup_datetime = pd.to_datetime(df["lpep_pickup_datetime"], errors="coerce")
    dropoff_datetime = pd.to_datetime(df["lpep_dropoff_datetime"], errors="coerce")
    duration_hours = (dropoff_datetime - pickup_datetime).dt.total_seconds() / 3600

    valid_mask = (
        (df["trip_distance"] > 0) & (duration_hours > 0) & duration_hours.notna()
    )

    speed = df.loc[valid_mask, "trip_distance"] / duration_hours.loc[valid_mask]
    speed = speed.replace([np.inf, -np.inf], np.nan).dropna()

    return {
        "p10": float(np.percentile(speed, 10)),
        "median": float(np.median(speed)),
        "p90": float(np.percentile(speed, 90)),
    }


def analyze_profiled_columns(
    df: pd.DataFrame, columns: list[str]
) -> dict[str, dict[str, float]]:
    """
    Calculate probabilities for categorical and discrete values
    """
    profile: dict[str, dict[str, float]] = {}

    for column in columns:
        counts = df[column].value_counts(normalize=True, dropna=False)

        profile[column] = {
            (
                "null"
                if (
                    value is None
                    or (isinstance(value, (float, np.floating)) and math.isnan(value))
                )
                else str(value)
            ): float(probability)
            for value, probability in counts.items()
        }

    return profile


def save_profile(profile: dict[str, Any], output_path: Path | str) -> None:
    """
    Save the generated profile as a JSON file
    """
    target_path = Path(output_path)
    target_path.parent.mkdir(parents=True, exist_ok=True)

    with target_path.open("w", encoding="utf-8") as file:
        json.dump(profile, file, indent=2)


def build_profile() -> dict[str, Any]:
    """
    Build the reference profile used by the event generator
    """
    df_raw = load_dataset(RAW_DATA_DIR)

    profile = {
        "hour_distribution": analyze_hour_distribution(df_raw),
        "trip_distance_values": build_reference_values(
            df_raw,
            "trip_distance",
            MAX_DISTANCE_POOL_SIZE,
            RANDOM_SEED,
        ),
        "fare_per_mile": analyze_fare_per_mile(df_raw),
        "tip_per_fare": analyze_tip_per_fare(df_raw),
        "speed_distribution": analyze_speed_distribution(df_raw),
        "category_distribution": analyze_profiled_columns(
            df_raw,
            PROFILED_COLUMNS,
        ),
    }

    save_profile(profile, BATCH_PROFILING)

    return profile


if __name__ == "__main__":
    build_profile()
