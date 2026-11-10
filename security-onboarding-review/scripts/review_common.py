"""Shared helpers for the offline Celery onboarding review.

Everything here runs against memory:// and cache+memory:// using the test
certificates under ../certs. No network or production service is involved.
"""
from __future__ import annotations

import glob
import importlib.util
import os
import sys

REVIEW_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
REPO_ROOT = os.path.dirname(REVIEW_ROOT)
# Use the celery source tree this review lives in.
sys.path.insert(0, REPO_ROOT)

CONFIG_DIR = os.path.join(REVIEW_ROOT, 'configs')
CERT_DIR = os.path.join(REVIEW_ROOT, 'certs')

CONFIGS = {
    'baseline': 'baseline.py',
    'task_only_counterexample': 'task_only_counterexample.py',
    'incomplete_trust_counterexample': 'incomplete_trust_counterexample.py',
}

# Constraint identifiers used in every report line.
C1 = 'C1'  # tasks only from the allowed signer
C2 = 'C2'  # results accepted only as JSON
C3 = 'C3'  # events are not proof of task completion


def load_config_module(name: str, env: dict | None = None):
    if env:
        os.environ.update(env)
    path = os.path.join(CONFIG_DIR, CONFIGS[name])
    spec = importlib.util.spec_from_file_location(f'review_cfg_{name}', path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def resolve_path(setting: str) -> str:
    """Resolve a config path (possibly globbed) against the review root."""
    if '*' in setting:
        base, pattern = setting.split('*', 1)
        return os.path.join(REVIEW_ROOT, base, '*' + pattern)
    return os.path.join(REVIEW_ROOT, setting)


def read_settings(module) -> dict:
    settings = {k: v for k, v in vars(module).items() if k.islower()}
    for key in ('security_key', 'security_certificate', 'security_cert_store'):
        if key in settings:
            settings[key] = resolve_path(settings[key])
    return settings


def build_app(name: str, settings: dict, setup_security: bool = True):
    from celery import Celery
    app = Celery(name)
    app.conf.update(settings)
    if setup_security:
        app.setup_security()
    return app


def glob_cert_store(pattern: str) -> list[str]:
    return sorted(glob.glob(pattern))


def root_cause(exc: BaseException) -> str:
    """Walk wrapped exceptions (kombu re-wraps SecurityError) to the root."""
    seen = set()
    current = exc
    while current.__cause__ is not None and id(current) not in seen:
        seen.add(id(current))
        current = current.__cause__
    return f'{type(current).__name__}: {current}'
