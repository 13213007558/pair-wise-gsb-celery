# Feedback task starting environment

Upstream: https://github.com/celery/celery
Upstream commit: `40a3154d457f085770ee7ed85ee892b4608cfb94`
Prepared on macOS with CPython 3.12.11. No external database, queue, or cloud account is required.
The dependency file pins the installed runtime and test packages for the selected baseline. It is not a claim that all optional upstream test suites are installed.

From the repository root:

```sh
export NO_INTERNET=1 TZ=UTC
uv venv --python 3.12 .venv
uv pip install --python .venv/bin/python -r .feedback/requirements.txt -e .
.venv/bin/python -m pytest -q -o addopts= t/unit/security
```

Scope note: The scope is the command below, not all optional upstream integrations.

Observed baseline (2026-11-10): `37 passed, 1 skipped in 6.52s`
Whole-command wall time: 7.870 seconds, exit code 0. This is one local measurement, not a runtime guarantee. The first install is excluded.

This seed adds preparation metadata only. Application sources and upstream tests are unchanged; there is no task solution in this commit.
