"""
Lê Parquet em data/raw/fake_transactions, exibe contagem e amostra, grava Delta em data/bronze/bronze_transactions.
Executar na raiz do repositório (CWD = diretório que contém a pasta data/).
"""

from __future__ import annotations

import sys
from pathlib import Path

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession


def _repo_root() -> Path:
    return Path(__file__).resolve().parent


def _wait_for_enter_before_exit() -> None:
    """Em terminal interativo (ex.: docker compose run -it), aguarda Enter antes de encerrar."""
    if not sys.stdin.isatty():
        return
    try:
        input("\nPressione Enter para encerrar o container... ")
    except EOFError:
        pass


def main() -> int:
    root = _repo_root()
    input_path = (root / "data" / "raw" / "fake_transactions").as_posix()
    output_path = (root / "data" / "bronze" / "bronze_transactions").as_posix()

    if not Path(input_path).is_dir():
        print(
            f"Erro: pasta de entrada não encontrada: {input_path}\n"
            "Execute a partir da raiz do projeto e confira se os Parquet existem.",
            file=sys.stderr,
        )
        return 1

    bronze_dir = Path(output_path).parent
    try:
        bronze_dir.mkdir(parents=True, exist_ok=True)
    except OSError as exc:
        print(f"Erro ao criar diretório bronze: {exc}", file=sys.stderr)
        return 1

    spark: SparkSession | None = None
    try:
        builder = (
            SparkSession.builder.appName("cdi_bonus_raw_to_bronze")
            .master("local[*]")
            .config("spark.sql.shuffle.partitions", "4")
        )
        spark = configure_spark_with_delta_pip(builder).getOrCreate()
    except Exception as exc:  # noqa: BLE001 - surface any Spark/Delta bootstrap failure
        print(f"Erro ao iniciar SparkSession com Delta: {exc}", file=sys.stderr)
        return 1

    try:
        df = spark.read.format("parquet").load(input_path)
        n = df.count()
        print(f"Total de linhas: {n}")
        df.show(20, truncate=False)

        (
            df.write.format("delta")
            .option("mergeSchema", "true")
            .mode("append")
            .save(output_path)
        )
        print(f"Delta gravado em: {output_path}")

    except Exception as exc:  # noqa: BLE001
        print(f"Erro na leitura/transformação/gravação: {exc}", file=sys.stderr)
        return 1
    finally:
        if spark is not None:
            spark.stop()

    return 0


if __name__ == "__main__":
    exit_code = main()
    _wait_for_enter_before_exit()
    raise SystemExit(exit_code)
