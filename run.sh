#!/usr/bin/env bash
set -euo pipefail

export FLASK_APP=app
exec flask run --no-debugger --no-reload
