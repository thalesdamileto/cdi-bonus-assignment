#!/bin/sh
set -eu
cd /workspace
poetry install --no-interaction --no-ansi
exec "$@"
