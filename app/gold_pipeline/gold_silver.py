from __future__ import annotations

from dataclasses import dataclass
from typing import Callable

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, current_timestamp, when

from app.helpers.general_helpers import log


@dataclass(frozen=True)
class GoldSilverConfig:
    silver_path: str
    gold_base_path: str
    account_id_column: str = "account_id"
    total_amount_column: str = "total_amount"
    amount_cdi_applicable_column: str = "amount_cdi_applicable"
    amount_cdi_not_applicable_column: str = "amount_cdi_not_applicable"
    balance_to_apply_cdi_column: str = "balance_to_apply_cdi"
    cdi_bonus_column: str = "cdi_bonus"
    final_balance_column: str = "final_balance"
    current_balance_column: str = "current_balance"
    last_day_bonus_column: str = "last_day_bonus"
    cdi_bonus_date_column: str = "cdi_bonus_date"
    cdi_rate: float = 0.00055
    decimal_precision: str = "decimal(18, 2)"
    audit_table_dir: str = "daily_cdi_stagin_audit"
    bonus_table_dir: str = "daily_cdi_bonus"
    account_balance_table_dir: str = "account_balance_last_day_cdi"


def _build_gold_frames(
    silver_df: DataFrame,
    config: GoldSilverConfig,
) -> tuple[DataFrame, DataFrame, DataFrame]:
    cdi_apply_df = silver_df.withColumn(
        config.balance_to_apply_cdi_column,
        when(
            col(config.amount_cdi_not_applicable_column) < 0,
            col(config.amount_cdi_applicable_column)
            + col(config.amount_cdi_not_applicable_column),
        ).otherwise(col(config.amount_cdi_applicable_column)),
    )

    cdi_apply_df = cdi_apply_df.withColumn(
        config.cdi_bonus_column,
        (col(config.balance_to_apply_cdi_column) * config.cdi_rate).cast(
            config.decimal_precision
        ),
    ).withColumn(
        config.final_balance_column,
        (col(config.cdi_bonus_column) + col(config.total_amount_column)).cast(
            config.decimal_precision
        ),
    )

    cdi_bonus_df = cdi_apply_df.select(
        col(config.account_id_column),
        col(config.cdi_bonus_column),
        current_timestamp().alias(config.cdi_bonus_date_column),
    )

    gold_account_balance_df = cdi_apply_df.select(
        col(config.account_id_column),
        col(config.final_balance_column).alias(config.current_balance_column),
        col(config.cdi_bonus_column).alias(config.last_day_bonus_column),
    )

    return cdi_apply_df, cdi_bonus_df, gold_account_balance_df


def _append_delta(df: DataFrame, path: str) -> None:
    df.write.format("delta").mode("append").option("mergeSchema", "true").save(path)


def _merge_account_balance(
    spark: SparkSession,
    df: DataFrame,
    config: GoldSilverConfig,
    target_path: str,
) -> None:
    if not DeltaTable.isDeltaTable(spark, target_path):
        df.write.format("delta").mode("overwrite").save(target_path)
        return

    target_table = DeltaTable.forPath(spark, target_path)
    target_table.alias("target").merge(
        source=df.alias("new_data"),
        condition=f"target.{config.account_id_column} = new_data.{config.account_id_column}",
    ).whenNotMatchedInsertAll().whenMatchedUpdate(
        set={
            config.current_balance_column: f"new_data.{config.current_balance_column}",
            config.last_day_bonus_column: f"new_data.{config.last_day_bonus_column}",
        }
    ).execute()


def run_gold_silver(spark: SparkSession, config: GoldSilverConfig) -> None:
    try:
        silver_df = spark.read.format("delta").load(config.silver_path)
        cdi_apply_df, cdi_bonus_df, gold_account_balance_df = _build_gold_frames(
            silver_df=silver_df,
            config=config,
        )

        audit_path = f"{config.gold_base_path}/{config.audit_table_dir}"
        bonus_path = f"{config.gold_base_path}/{config.bonus_table_dir}"
        account_balance_path = (
            f"{config.gold_base_path}/{config.account_balance_table_dir}"
        )

        _append_delta(cdi_apply_df, audit_path)
        _append_delta(cdi_bonus_df, bonus_path)
        _merge_account_balance(
            spark=spark,
            df=gold_account_balance_df,
            config=config,
            target_path=account_balance_path,
        )
        log(f"Gold step completed: {config.silver_path} -> {config.gold_base_path}")
    except Exception as error:
        log(f"Gold step failed: {error}", level="error")
        raise


def get_gold_silver_tasks(config: GoldSilverConfig) -> dict[str, Callable[[SparkSession], None]]:
    return {
        "run_gold_silver": lambda spark: run_gold_silver(spark=spark, config=config),
    }
