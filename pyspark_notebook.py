from pathlib import Path

from app.helpers.spark_helpers import create_spark_session_with_delta, ensure_java_home
from app.helpers.general_helpers import log

bronze_path = "data/bronze/bronze_transactions"
silver_path = "data/silver/transactions"
gold_account_balance_path = "data/gold/transactions/account_balance_last_day_cdi"
gold_cdi_bonus_path = "data/gold/transactions/daily_cdi_bonus"
gold_cdi_stagin_audit_path = "data/gold/transactions/daily_cdi_stagin_audit"


def main() -> int:
    ensure_java_home()

    root = Path(__file__).resolve().parent
    input_glob = (root / "data" / "raw" / "fake_transactions" / "part-*.parquet").as_posix()

    spark = create_spark_session_with_delta()
    try:
        df_raw = spark.read.format("parquet").load(input_glob)
        df_raw.show(5) # df_raw.sample(0.01).show()
        log(f"Total rows raw: {df_raw.count()}")
        

        df_bronze = spark.read.format("delta").load(bronze_path)
        df_bronze.show(5)
        log(f"Total rows bronze: {df_bronze.count()}")
        
        df_silver = spark.read.format("delta").load(silver_path)
        df_silver.show(5)
        log(f"Total rows silver: {df_silver.count()}")
        
        df_gold_account_balance = spark.read.format("delta").load(gold_account_balance_path)
        df_gold_account_balance.show(5)
        log(f"Total rows gold account balance: {df_gold_account_balance.count()}")
        
        df_gold_cdi_bonus = spark.read.format("delta").load(gold_cdi_bonus_path)
        df_gold_cdi_bonus.show(5)
        log(f"Total rows gold cdi bonus: {df_gold_cdi_bonus.count()}")
        
        df_gold_cdi_apply = spark.read.format("delta").load(gold_cdi_stagin_audit_path)
        df_gold_cdi_apply.show(5)
        log(f"Total rows gold cdi apply: {df_gold_cdi_apply.count()}")
        


    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
