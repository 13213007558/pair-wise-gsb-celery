#!/usr/bin/env bash
# Offline review entry point. Uses the repository virtualenv if present,
# otherwise a python3 that has celery + cryptography importable.
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
PYTHON="$HERE/../.venv/bin/python"
[ -x "$PYTHON" ] || PYTHON="$(command -v python3)"

echo '### 1/2 static configuration check (non-zero exit means a counterexample'
echo '###       contains its intended violation - expected for this material set)'
set +e
"$PYTHON" "$HERE/scripts/static_check.py"
STATIC_RC=$?
set -e
echo
echo '### 2/2 runtime verification (in-memory transport, test certificates)'
"$PYTHON" "$HERE/scripts/runtime_checks.py"
RUNTIME_RC=$?
echo
echo "static_check rc=$STATIC_RC (1 expected: two configs are deliberate counterexamples)"
echo "runtime_checks rc=$RUNTIME_RC (0 expected: all demonstrated behaviors hold)"
exit "$RUNTIME_RC"
