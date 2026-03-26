"""Tests for Silver/Gold DataFrame transforms (no Delta writes)."""

from __future__ import annotations

from decimal import Decimal

from pyspark.sql import Row, SparkSession

from app.gold_pipeline.gold_silver import DEFAULT_GOLD_SILVER_CONFIG, _build_gold_frames
from app.silver_pipeline.local_silver import DEFAULT_LOCAL_SILVER_CONFIG, _build_silver_delta


def test_build_silver_delta_splits_by_time_limit(spark_session: SparkSession) -> None:
    cfg = dict(DEFAULT_LOCAL_SILVER_CONFIG)
    cfg["time_limit"] = "2024-10-06T00:00:00.000"

    rows = [
        Row(account_id="a1", event_time="2024-10-05T12:00:00.000", amount=50.0),
        Row(account_id="a1", event_time="2024-10-07T12:00:00.000", amount=30.0),
    ]
    df = spark_session.createDataFrame(rows)
    out = _build_silver_delta(df, cfg).orderBy("account_id").collect()

    assert len(out) == 1
    r = out[0]
    assert r["account_id"] == "a1"
    assert r["amount_cdi_applicable"] == 50.0
    assert r["amount_cdi_not_applicable"] == 30.0
    assert r["total_amount"] == 80.0


def test_build_gold_frames_negative_not_applicable_adjusts_base(spark_session: SparkSession) -> None:
    cfg = dict(DEFAULT_GOLD_SILVER_CONFIG)
    cfg["cdi_rate"] = 0.00055

    rows = [
        Row(
            account_id="x",
            total_amount=150.0,
            amount_cdi_applicable=200.0,
            amount_cdi_not_applicable=-50.0,
        )
    ]
    silver_df = spark_session.createDataFrame(rows)
    cdi_apply, _, _ = _build_gold_frames(silver_df, cfg)
    row = cdi_apply.collect()[0]

    # balance_to_apply_cdi = applicable + not_applicable when not_applicable < 0
    assert row["balance_to_apply_cdi"] == 150.0
    assert row["cdi_bonus"] == Decimal("0.08")  # 150 * 0.00055 -> 0.0825 rounded to 2 dp
    assert row["final_balance"] == Decimal("150.08")


def test_build_gold_frames_bonus_zero_below_hundred_threshold(spark_session: SparkSession) -> None:
    cfg = dict(DEFAULT_GOLD_SILVER_CONFIG)

    rows = [
        Row(
            account_id="y",
            total_amount=80.0,
            amount_cdi_applicable=80.0,
            amount_cdi_not_applicable=0.0,
        )
    ]
    silver_df = spark_session.createDataFrame(rows)
    cdi_apply, bonus_df, balance_df = _build_gold_frames(silver_df, cfg)
    r = cdi_apply.collect()[0]

    assert r["balance_to_apply_cdi"] == 80.0
    assert r["cdi_bonus"] == Decimal("0.00")
    assert r["final_balance"] == Decimal("80.00")

    b = bonus_df.collect()[0]
    assert b["account_id"] == "y"
    assert b["cdi_bonus"] == Decimal("0.00")

    bal = balance_df.collect()[0]
    assert bal["current_balance"] == Decimal("80.00")
    assert bal["last_day_bonus"] == Decimal("0.00")
