from __future__ import annotations

from pathlib import Path

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

from app.bronze_pipeline.local_bronze import LocalBronzeConfig, get_local_bronze_tasks
from app.gold_pipeline.gold_silver import GoldSilverConfig, get_gold_silver_tasks
from app.helpers.general_helpers import log
from app.helpers.spark_helpers import ensure_java_home
from app.silver_pipeline.local_silver import get_local_silver_tasks


def _create_spark_session() -> SparkSession:
    builder = (
        SparkSession.builder.appName("cdi-bonus-assignment")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


def main() -> int:
    spark: SparkSession | None = None
    try:
        ensure_java_home()
        spark = _create_spark_session()

        repo_root = Path(__file__).resolve().parent.parent
        bronze_config = LocalBronzeConfig(
            source_path=(repo_root / "data" / "raw" / "fake_transactions" / "part-*.parquet").as_posix(),
            bronze_path=(repo_root / "data" / "bronze" / "bronze_transactions").as_posix(),
        )
        silver_config = {
            "bronze_path": bronze_config.bronze_path,
            "silver_path": (repo_root / "data" / "silver" / "transactions").as_posix(),
        }
        gold_config = GoldSilverConfig(
            silver_path=str(silver_config["silver_path"]),
            gold_base_path=(repo_root / "data" / "gold" / "transactions").as_posix(),
        )

        for task in get_local_bronze_tasks(bronze_config).values():
            task(spark)
        for task in get_local_silver_tasks(silver_config).values():
            task(spark)
        for task in get_gold_silver_tasks(gold_config).values():
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