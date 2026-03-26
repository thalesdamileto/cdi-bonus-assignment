"""Tests for pipeline paths resolved from the repository root."""

from __future__ import annotations

from pathlib import Path

from app.main import PIPELINE_CONFIG, _pipeline_stages_with_repo_paths


def test_pipeline_stages_use_posix_paths_under_repo_root(tmp_path: Path) -> None:
    bronze, silver, gold = _pipeline_stages_with_repo_paths(tmp_path)

    assert bronze["source_path"] == (tmp_path / "data" / "raw" / "fake_transactions" / "part-*.parquet").as_posix()
    assert bronze["bronze_path"] == (tmp_path / "data" / "bronze" / "bronze_transactions").as_posix()

    assert silver["bronze_path"] == bronze["bronze_path"]
    assert silver["silver_path"] == (tmp_path / "data" / "silver" / "transactions").as_posix()

    assert gold["silver_path"] == silver["silver_path"]
    assert gold["gold_base_path"] == (tmp_path / "data" / "gold" / "transactions").as_posix()


def test_pipeline_stages_preserve_non_path_config() -> None:
    repo = Path("/tmp/fake-repo")
    bronze, silver, gold = _pipeline_stages_with_repo_paths(repo)

    assert bronze["source_format"] == PIPELINE_CONFIG["bronze"]["source_format"]
    assert bronze["merge_schema"] == PIPELINE_CONFIG["bronze"]["merge_schema"]
    assert silver["time_limit"] == PIPELINE_CONFIG["silver"]["time_limit"]
    assert gold["cdi_rate"] == PIPELINE_CONFIG["gold"]["cdi_rate"]
