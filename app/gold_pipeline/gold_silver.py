from __future__ import annotations

from typing import Callable

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, current_timestamp, lit, when

from app.helpers.general_helpers import log


DEFAULT_GOLD_SILVER_CONFIG: dict[str, str | float] = {
    "silver_path": "data/silver/transactions",
    "gold_base_path": "data/gold/transactions",
    "account_id_column": "account_id",
    "total_amount_column": "total_amount",
    "amount_cdi_applicable_column": "amount_cdi_applicable",
    "amount_cdi_not_applicable_column": "amount_cdi_not_applicable",
    "balance_to_apply_cdi_column": "balance_to_apply_cdi",
    "cdi_bonus_column": "cdi_bonus",
    "final_balance_column": "final_balance",
    "current_balance_column": "current_balance",
    "last_day_bonus_column": "last_day_bonus",
    "cdi_bonus_date_column": "cdi_bonus_date",
    "cdi_rate": 0.00055,
    "decimal_precision": "decimal(18, 2)",
    "audit_table_dir": "daily_cdi_stagin_audit",
    "bonus_table_dir": "daily_cdi_bonus",
    "account_balance_table_dir": "account_balance_last_day_cdi",
}


def _build_gold_frames(
    silver_df: DataFrame,
    config: dict[str, str | float],
) -> tuple[DataFrame, DataFrame, DataFrame]:
    """Create audit, bonus, and account-balance DataFrames from Silver data."""
    account_id_column = str(config["account_id_column"])
    total_amount_column = str(config["total_amount_column"])
    amount_cdi_applicable_column = str(config["amount_cdi_applicable_column"])
    amount_cdi_not_applicable_column = str(config["amount_cdi_not_applicable_column"])
    balance_to_apply_cdi_column = str(config["balance_to_apply_cdi_column"])
    cdi_bonus_column = str(config["cdi_bonus_column"])
    final_balance_column = str(config["final_balance_column"])
    current_balance_column = str(config["current_balance_column"])
    last_day_bonus_column = str(config["last_day_bonus_column"])
    cdi_bonus_date_column = str(config["cdi_bonus_date_column"])
    cdi_rate = float(config["cdi_rate"])
    decimal_precision = str(config["decimal_precision"])

    cdi_apply_df = silver_df.withColumn( # table with account, balance_to_apply_cdi, total_amount, amount_cdi_applicable, amount_cdi_not_applicable, need to calculate the cdi bonus
        balance_to_apply_cdi_column,
        when(
            col(amount_cdi_not_applicable_column) < 0,
            col(amount_cdi_applicable_column) + col(amount_cdi_not_applicable_column), # if amount_cdi_not_applicable is negative, add it to the amount_cdi_applicable
        ).otherwise(col(amount_cdi_applicable_column)),
    )

    cdi_apply_df = cdi_apply_df.withColumn(
        cdi_bonus_column,
        when(
            col(balance_to_apply_cdi_column) > 100, # $100 minimum balance to apply cdi
            col(balance_to_apply_cdi_column) * cdi_rate,
        )
        .otherwise(lit(0)) # no cdi bonus if balance is less than $100
        .cast(decimal_precision),
    ).withColumn(
        final_balance_column,
        (col(cdi_bonus_column) + col(total_amount_column)).cast(decimal_precision),
    )

    cdi_bonus_df = cdi_apply_df.select( # table with account, cdi_bonus and cdi_bonus_date, need to Federal Taxes calculations
        col(account_id_column),
        col(cdi_bonus_column),
        current_timestamp().alias(cdi_bonus_date_column),
    )

    gold_account_balance_df = cdi_apply_df.select( # table with account, current_balance and last_day_bonus , need to merge with the current table to update the balance and last bonus for each account
        col(account_id_column),
        col(final_balance_column).alias(current_balance_column),
        col(cdi_bonus_column).alias(last_day_bonus_column),
    )

    return cdi_apply_df, cdi_bonus_df, gold_account_balance_df


def _append_delta(df: DataFrame, path: str) -> None:
    """Append a DataFrame to a Delta path with schema merge enabled."""
    df.write.format("delta").mode("append").option("mergeSchema", "true").save(path)


def _merge_account_balance(
    spark: SparkSession,
    df: DataFrame,
    account_id_column: str,
    current_balance_column: str,
    last_day_bonus_column: str,
    target_path: str,
) -> None:
    """Upsert current account balance and last bonus into the Gold table."""
    if not DeltaTable.isDeltaTable(spark, target_path):
        df.write.format("delta").mode("overwrite").save(target_path)
        return

    target_table = DeltaTable.forPath(spark, target_path)
    target_table.alias("target").merge(
        source=df.alias("new_data"),
        condition=f"target.{account_id_column} = new_data.{account_id_column}",
    ).whenNotMatchedInsertAll().whenMatchedUpdate(
        set={
            current_balance_column: f"new_data.{current_balance_column}",
            last_day_bonus_column: f"new_data.{last_day_bonus_column}",
        }
    ).execute()


def run_gold_silver(spark: SparkSession, config: dict[str, str | float]) -> None:
    """Execute Gold processing: read Silver, compute CDI, and persist outputs."""
    try:
        silver_path = str(config["silver_path"])
        gold_base_path = str(config["gold_base_path"])
        audit_table_dir = str(config["audit_table_dir"])
        bonus_table_dir = str(config["bonus_table_dir"])
        account_balance_table_dir = str(config["account_balance_table_dir"])
        account_id_column = str(config["account_id_column"])
        current_balance_column = str(config["current_balance_column"])
        last_day_bonus_column = str(config["last_day_bonus_column"])

        silver_df = spark.read.format("delta").load(silver_path)
        cdi_apply_df, cdi_bonus_df, gold_account_balance_df = _build_gold_frames(
            silver_df=silver_df,
            config=config,
        )

        audit_path = f"{gold_base_path}/{audit_table_dir}"
        bonus_path = f"{gold_base_path}/{bonus_table_dir}"
        account_balance_path = f"{gold_base_path}/{account_balance_table_dir}"

        _append_delta(cdi_apply_df, audit_path)
        _append_delta(cdi_bonus_df, bonus_path)
        _merge_account_balance(
            spark=spark,
            df=gold_account_balance_df,
            account_id_column=account_id_column,
            current_balance_column=current_balance_column,
            last_day_bonus_column=last_day_bonus_column,
            target_path=account_balance_path,
        )
        log(f"Gold step completed: {silver_path} -> {gold_base_path}")
    except Exception as error:
        log(f"Gold step failed: {error}", level="error")
        raise


def get_gold_silver_tasks(
    config: dict[str, str | float] | None = None,
) -> dict[str, Callable[[SparkSession], None]]:
    """Return the Gold task map expected by the pipeline orchestrator."""
    final_config = dict(DEFAULT_GOLD_SILVER_CONFIG)
    if config:
        final_config.update(config)

    return {
        "run_gold_silver": lambda spark: run_gold_silver(
            spark=spark,
            config=final_config,
        ),
    }
