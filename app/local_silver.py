from __future__ import annotations

from typing import Callable

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from app.helpers.general_helpers import log


DEFAULT_LOCAL_SILVER_CONFIG: dict[str, str] = {
    "bronze_path": "data/bronze/bronze_transactions",
    "silver_path": "data/silver/transactions",
    "source_format": "delta",
    "target_format": "delta",
    "write_mode_if_missing": "overwrite",
    "time_limit": "2024-10-06T00:00:00.000",
    "account_id_col": "account_id",
    "event_time_col": "event_time",
    "amount_col": "amount",
    "cdi_applicable_col": "cdi_applicable",
    "amount_cdi_applicable_col": "amount_cdi_applicable",
    "amount_cdi_not_applicable_col": "amount_cdi_not_applicable",
    "total_amount_col": "total_amount",
}


def _build_final_delta_df(df_batch: DataFrame, config: dict[str, str]) -> DataFrame:
    cdi_applicable_col = config["cdi_applicable_col"]
    event_time_col = config["event_time_col"]
    time_limit = config["time_limit"]
    account_id_col = config["account_id_col"]
    amount_col = config["amount_col"]
    amount_cdi_applicable_col = config["amount_cdi_applicable_col"]
    amount_cdi_not_applicable_col = config["amount_cdi_not_applicable_col"]
    total_amount_col = config["total_amount_col"]

    eligible_transactions_df = df_batch.withColumn(
        cdi_applicable_col,
        (F.col(event_time_col) < F.lit(time_limit)).cast("boolean"),
    )

    grouped_df = eligible_transactions_df.groupBy(
        account_id_col,
        cdi_applicable_col,
    ).agg(F.sum(amount_col).alias("amount_sum"))

    pivoted_df = grouped_df.groupBy(account_id_col).pivot(
        cdi_applicable_col,
        [True, False],
    ).agg(F.sum("amount_sum"))

    final_df = (
        pivoted_df.withColumn(
            amount_cdi_applicable_col,
            F.coalesce(F.col("true"), F.lit(0.0)),
        )
        .withColumn(
            amount_cdi_not_applicable_col,
            F.coalesce(F.col("false"), F.lit(0.0)),
        )
        .withColumn(
            total_amount_col,
            F.col(amount_cdi_applicable_col) + F.col(amount_cdi_not_applicable_col),
        )
        .select(
            account_id_col,
            total_amount_col,
            amount_cdi_applicable_col,
            amount_cdi_not_applicable_col,
        )
    )
    return final_df


def run_local_silver(
    spark: SparkSession,
    bronze_path: str,
    silver_path: str,
    source_format: str,
    target_format: str,
    write_mode_if_missing: str,
    time_limit: str,
    account_id_col: str,
    event_time_col: str,
    amount_col: str,
    cdi_applicable_col: str,
    amount_cdi_applicable_col: str,
    amount_cdi_not_applicable_col: str,
    total_amount_col: str,
) -> None:
    try:
        config = {
            "time_limit": time_limit,
            "account_id_col": account_id_col,
            "event_time_col": event_time_col,
            "amount_col": amount_col,
            "cdi_applicable_col": cdi_applicable_col,
            "amount_cdi_applicable_col": amount_cdi_applicable_col,
            "amount_cdi_not_applicable_col": amount_cdi_not_applicable_col,
            "total_amount_col": total_amount_col,
        }
        bronze_df = spark.read.format(source_format).load(bronze_path)
        final_delta_df = _build_final_delta_df(bronze_df, config)

        if not DeltaTable.isDeltaTable(spark, silver_path):
            final_delta_df.write.format(target_format).mode(write_mode_if_missing).save(silver_path)
            log(f"Silver step completed (new target): {silver_path}")
            return

        target_df = spark.read.format(target_format).load(silver_path).select(
            account_id_col,
            total_amount_col,
            amount_cdi_applicable_col,
            amount_cdi_not_applicable_col,
        )

        union_df = target_df.unionByName(final_delta_df)
        merged_source_df = union_df.groupBy(account_id_col).agg(
            F.sum(total_amount_col).alias(total_amount_col),
            F.sum(amount_cdi_applicable_col).alias(amount_cdi_applicable_col),
            F.sum(amount_cdi_not_applicable_col).alias(amount_cdi_not_applicable_col),
        )

        target_table = DeltaTable.forPath(spark, silver_path)
        merge_condition = f"target.{account_id_col} = source.{account_id_col}"
        update_map = {
            total_amount_col: f"source.{total_amount_col}",
            amount_cdi_applicable_col: f"source.{amount_cdi_applicable_col}",
            amount_cdi_not_applicable_col: f"source.{amount_cdi_not_applicable_col}",
        }
        insert_map = {
            account_id_col: f"source.{account_id_col}",
            **update_map,
        }

        target_table.alias("target").merge(
            source=merged_source_df.alias("source"),
            condition=merge_condition,
        ).whenMatchedUpdate(set=update_map).whenNotMatchedInsert(values=insert_map).execute()
        log(f"Silver step completed (merge): {bronze_path} -> {silver_path}")

    except Exception as error:
        log(f"Silver step failed: {error}", level="error")
        raise


def get_local_silver_tasks(
    config: dict[str, str] | None = None,
) -> dict[str, Callable[[SparkSession], None]]:
    final_config = dict(DEFAULT_LOCAL_SILVER_CONFIG)
    if config:
        final_config.update(config)

    bronze_path = str(final_config["bronze_path"])
    silver_path = str(final_config["silver_path"])
    source_format = str(final_config["source_format"])
    target_format = str(final_config["target_format"])
    write_mode_if_missing = str(final_config["write_mode_if_missing"])
    time_limit = str(final_config["time_limit"])
    account_id_col = str(final_config["account_id_col"])
    event_time_col = str(final_config["event_time_col"])
    amount_col = str(final_config["amount_col"])
    cdi_applicable_col = str(final_config["cdi_applicable_col"])
    amount_cdi_applicable_col = str(final_config["amount_cdi_applicable_col"])
    amount_cdi_not_applicable_col = str(final_config["amount_cdi_not_applicable_col"])
    total_amount_col = str(final_config["total_amount_col"])

    return {
        "silver_from_bronze_to_accounts_balance_cdi": lambda spark: run_local_silver(
            spark=spark,
            bronze_path=bronze_path,
            silver_path=silver_path,
            source_format=source_format,
            target_format=target_format,
            write_mode_if_missing=write_mode_if_missing,
            time_limit=time_limit,
            account_id_col=account_id_col,
            event_time_col=event_time_col,
            amount_col=amount_col,
            cdi_applicable_col=cdi_applicable_col,
            amount_cdi_applicable_col=amount_cdi_applicable_col,
            amount_cdi_not_applicable_col=amount_cdi_not_applicable_col,
            total_amount_col=total_amount_col,
        ),
    }
