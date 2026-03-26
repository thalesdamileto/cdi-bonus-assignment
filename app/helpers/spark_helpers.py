from __future__ import annotations

import os
import shutil
import sys
from pathlib import Path

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession


def create_spark_session_with_delta(app_name: str = "cdi-bonus-assignment") -> SparkSession:
    """SparkSession with Delta Lake JARs (delta-spark pip) and SQL extensions."""
    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()

def _user_local_jdk_17() -> Path | None:
    """JDK instalado em ~/.local/share/java/jdk-17* (ex.: tarball Temurin, sem apt)."""
    base = Path.home() / ".local" / "share" / "java"
    if not base.is_dir():
        return None
    candidates = sorted(base.glob("jdk-17*"), key=lambda p: p.name, reverse=True)
    for jdk in candidates:
        if (jdk / "bin" / "java").is_file():
            return jdk
    return None


def ensure_java_home() -> None:
    """PySpark exige JAVA_HOME; usa variável, JDK em ~/.local/share/java ou `java` no PATH."""
    existing = os.environ.get("JAVA_HOME")
    if existing and (Path(existing) / "bin" / "java").is_file():
        return
    local = _user_local_jdk_17()
    if local is not None:
        os.environ["JAVA_HOME"] = str(local)
        return
    java_bin = shutil.which("java")
    if not java_bin:
        print(
            "Erro: Java não encontrado. Opções:\n"
            "  sudo apt update && sudo apt install -y openjdk-17-jdk-headless\n"
            "  ou instale Temurin 17 em ~/.local/share/java/ (jdk-17*)\n"
            "Depois defina JAVA_HOME ou garanta `java` no PATH.",
            file=sys.stderr,
        )
        sys.exit(1)
    jdk_root = Path(java_bin).resolve().parent.parent
    if not (jdk_root / "bin" / "java").is_file():
        print(
            "Erro: defina JAVA_HOME manualmente (ex.: /usr/lib/jvm/java-17-openjdk-amd64).",
            file=sys.stderr,
        )
        sys.exit(1)
    os.environ["JAVA_HOME"] = str(jdk_root)