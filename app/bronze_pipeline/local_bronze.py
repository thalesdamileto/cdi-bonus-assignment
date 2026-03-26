from __future__ import annotations

from dataclasses import dataclass
from typing import Callable

from pyspark.sql import SparkSession

from app.helpers.general_helpers import log


@dataclass(frozen=True)
class LocalBronzeConfig:
    source_path: str
    bronze_path: str
    source_format: str = "parquet"
    target_format: str = "delta"
    write_mode: str = "append"
    merge_schema: bool = True


def run_local_bronze(spark: SparkSession, config: LocalBronzeConfig) -> None:
    try:
        bronze_df = spark.read.format(config.source_format).load(config.source_path)
        bronze_df.write.format(config.target_format).option(
            "mergeSchema",
            str(config.merge_schema).lower(),
        ).mode(config.write_mode).save(config.bronze_path)
        log(f"Bronze step completed: {config.source_path} -> {config.bronze_path}")
    except Exception as error:
        log(f"Bronze step failed: {error}", level="error")
        raise


def get_local_bronze_tasks(config: LocalBronzeConfig) -> dict[str, Callable[[SparkSession], None]]:
    return {
        "run_local_bronze": lambda spark: run_local_bronze(spark=spark, config=config),
    }
