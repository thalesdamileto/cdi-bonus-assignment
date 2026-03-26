from __future__ import annotations

import json
from pathlib import Path

from pyspark.sql import SparkSession

from app.bronze_pipeline.local_bronze import get_local_bronze_tasks
from app.gold_pipeline.gold_silver import get_gold_silver_tasks
from app.helpers.general_helpers import log
from app.helpers.spark_helpers import create_spark_session_with_delta, ensure_java_home
from app.silver_pipeline.local_silver import get_local_silver_tasks

# Canonical pipeline settings (mirrors DEFAULT_* in bronze/silver/gold modules). Valid JSON.
_PIPELINE_CONFIG_JSON = """
{
  "bronze": {
    "source_path": "data/raw/fake_transactions/part-*.parquet",
    "bronze_path": "data/bronze/bronze_transactions",
    "source_format": "parquet",
    "target_format": "delta",
    "write_mode": "append",
    "merge_schema": true
  },
  "silver": {
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
    "total_amount_col": "total_amount"
  },
  "gold": {
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
    "account_balance_table_dir": "account_balance_last_day_cdi"
  }
}
"""

PIPELINE_CONFIG: dict[str, dict[str, object]] = json.loads(_PIPELINE_CONFIG_JSON)


def _pipeline_stages_with_repo_paths(
    repo_root: Path,
) -> tuple[dict[str, object], dict[str, object], dict[str, object]]:
    """Shallow-copy each stage and replace path keys with absolute paths under repo_root."""
    bronze = dict(PIPELINE_CONFIG["bronze"])
    silver = dict(PIPELINE_CONFIG["silver"])
    gold = dict(PIPELINE_CONFIG["gold"])

    bronze["source_path"] = (
        repo_root / "data" / "raw" / "fake_transactions" / "part-*.parquet"
    ).as_posix()
    bronze["bronze_path"] = (repo_root / "data" / "bronze" / "bronze_transactions").as_posix()

    silver["bronze_path"] = str(bronze["bronze_path"])
    silver["silver_path"] = (repo_root / "data" / "silver" / "transactions").as_posix()

    gold["silver_path"] = str(silver["silver_path"])
    gold["gold_base_path"] = (repo_root / "data" / "gold" / "transactions").as_posix()

    return bronze, silver, gold


def main() -> int:
    spark: SparkSession | None = None
    try:
        ensure_java_home()
        spark = create_spark_session_with_delta()

        repo_root = Path(__file__).resolve().parent.parent
        bronze_cfg, silver_cfg, gold_cfg = _pipeline_stages_with_repo_paths(repo_root)

        for task in get_local_bronze_tasks(bronze_cfg).values():
            task(spark)
        for task in get_local_silver_tasks(silver_cfg).values():
            task(spark)
        for task in get_gold_silver_tasks(gold_cfg).values():
            task(spark)

        log("Local Bronze -> Silver -> Gold pipeline completed.")
        return 0
    except Exception as error:
        log(f"Pipeline failed: {error}", level="error")
        raise
    finally:
        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
