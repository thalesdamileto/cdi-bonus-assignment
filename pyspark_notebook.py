from pathlib import Path

from pyspark.sql import SparkSession
from app.helpers.spark_helpers import ensure_java_home
from app.helpers.general_helpers import log


def main() -> int:
    ensure_java_home()

    root = Path(__file__).resolve().parent
    input_glob = (root / "data" / "raw" / "fake_transactions" / "part-*.parquet").as_posix()

    spark = SparkSession.builder.appName("cdi-bonus-assignment").getOrCreate()
    try:
        df_raw = spark.read.format("parquet").load(input_glob)
        df_raw.show(5) # df_raw.sample(0.01).show()
        log(f"Total rows: {df_raw.count()}")
        
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
