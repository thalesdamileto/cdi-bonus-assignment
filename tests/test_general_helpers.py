"""Tests for small shared helpers."""

from __future__ import annotations

from app.helpers.general_helpers import log


def test_log_default_level(capsys) -> None:
    log("hello")
    captured = capsys.readouterr()
    assert captured.out == "INFO: hello\n"


def test_log_error_level(capsys) -> None:
    log("oops", level="error")
    captured = capsys.readouterr()
    assert captured.out == "ERROR: oops\n"
