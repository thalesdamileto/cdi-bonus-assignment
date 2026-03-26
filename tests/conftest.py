"""Shared pytest fixtures (Spark + Delta for transform tests)."""

from __future__ import annotations

import pytest

from app.helpers.spark_helpers import create_spark_session_with_delta, ensure_java_home


@pytest.fixture(scope="session")
def spark_session():
    """One SparkSession per test session; Delta extensions loaded via delta-spark."""
    ensure_java_home()
    spark = create_spark_session_with_delta(app_name="cdi-bonus-assignment-tests")
    try:
        yield spark
    finally:
        spark.stop()
