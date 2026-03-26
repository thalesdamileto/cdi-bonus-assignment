from __future__ import annotations

from typing import Callable

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

from app.helpers.general_helpers import log


DEFAULT_LOCAL_BRONZE_CONFIG: dict[str, str | bool] = {
    "source_path": "data/raw/fake_transactions/part-*.parquet",
    "bronze_path": "data/bronze/bronze_transactions",
    "quarantine_path": "data/bronze/quarantine",
    "source_format": "parquet",
    "target_format": "delta",
    "write_mode": "append",
    "merge_schema": True,
}


def run_local_bronze(
    spark: SparkSession,
    source_path: str,
    bronze_path: str,
    quarantine_path: str,
    source_format: str,
    target_format: str,
    write_mode: str,
    merge_schema: bool,
) -> None:
    """Read raw transactions and append them to the Bronze Delta path."""
    try:
        bronze_df = spark.read.format(source_format).load(source_path)

        valid_transaction_types = [
            "WITHDRAWAL",
            "TRANSFER_OUT",
            "TRANSFER_IN",
            "DEPOSIT",
            "WALLET_CREATED",
        ]

        invalid_rows_condition = (
            col("user_id").isNull()
            | col("account_id").isNull()
            | ~col("transaction_type").isin(valid_transaction_types)
        )

        quarantine_df = bronze_df.filter(invalid_rows_condition)
        bronze_df = bronze_df.filter(~invalid_rows_condition)

        quarantine_df.write.format(target_format).option(
            "mergeSchema",
            str(merge_schema).lower(),
        ).mode(write_mode).save(quarantine_path)

        bronze_df.write.format(target_format).option( # write the data to the bronze table
            "mergeSchema",
            str(merge_schema).lower(),
        ).mode(write_mode).save(bronze_path)
        log(f"Bronze step completed: {source_path} -> {bronze_path}")
    except Exception as error:
        log(f"Bronze step failed: {error}", level="error")
        raise


def get_local_bronze_tasks(
    config: dict[str, str | bool] | None = None,
) -> dict[str, Callable[[SparkSession], None]]:
    """Return the Bronze task map expected by the pipeline orchestrator."""
    final_config = dict(DEFAULT_LOCAL_BRONZE_CONFIG)
    if config:
        final_config.update(config)

    source_path = str(final_config["source_path"])
    bronze_path = str(final_config["bronze_path"])
    quarantine_path = str(final_config["quarantine_path"])
    source_format = str(final_config["source_format"])
    target_format = str(final_config["target_format"])
    write_mode = str(final_config["write_mode"])
    merge_schema = bool(final_config["merge_schema"])

    return {
        "run_local_bronze": lambda spark: run_local_bronze(
            spark=spark,
            source_path=source_path,
            bronze_path=bronze_path,
            quarantine_path=quarantine_path,
            source_format=source_format,
            target_format=target_format,
            write_mode=write_mode,
            merge_schema=merge_schema,
        ),
    }
