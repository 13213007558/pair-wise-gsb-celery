#!/usr/bin/env python3
"""Offline static configuration check for the Celery onboarding review.

It inspects the three config files and the test certificate material without
starting a worker, opening a broker connection, or contacting any service.

Each finding is reported against an explicit constraint:
  C1 - tasks accepted only from the allowed signer
  C2 - result backend accepts JSON only
  C3 - events must not be treated as completion proof

Findings come in three kinds:
  [PASS-STATIC]  verifiable from files and source right now
  [VIOLATION]    the config breaks a stated constraint
  [RUNTIME-ONLY] cannot be settled statically; runtime_checks.py proves it

A clean static report does NOT mean the deployment is secure: certificate
expiry, private-key permissions, broker/result-store access control, and the
application's event-handling discipline are outside what static files prove.
"""
from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from review_common import (C1, C2, C3, CONFIGS, glob_cert_store,
                           load_config_module, read_settings)

PASS = 'PASS-STATIC'
VIOLATION = 'VIOLATION'
RUNTIME = 'RUNTIME-ONLY'


class Report:
    def __init__(self):
        self.violations = 0
        self.runtime_items = 0
        self.passes = 0

    def line(self, kind: str, constraint: str, message: str):
        print(f'  [{kind}] {constraint}: {message}')
        if kind == VIOLATION:
            self.violations += 1
        elif kind == RUNTIME:
            self.runtime_items += 1
        else:
            self.passes += 1


def check_config(name: str, env: dict | None = None) -> Report:
    print(f'\n=== {name} ===')
    module = load_config_module(name, env)
    settings = read_settings(module)
    raw = {k: v for k, v in vars(module).items() if k.islower()}
    report = Report()

    # ---- C1: task-message signing posture --------------------------------
    if settings.get('task_serializer') == 'auth':
        report.line(PASS, C1, "task_serializer == 'auth' (tasks are signed)")
    else:
        report.line(VIOLATION, C1,
                    f"task_serializer is {settings.get('task_serializer')!r}, not 'auth'")

    if settings.get('accept_content') == ['auth']:
        report.line(PASS, C1,
                    "accept_content == ['auth'] (unsigned task content types refused)")
    else:
        report.line(VIOLATION, C1,
                    f'accept_content = {settings.get("accept_content")!r}; '
                    "must be exactly ['auth']")

    for key in ('security_key', 'security_certificate', 'security_cert_store'):
        path = settings.get(key)
        if not path:
            report.line(VIOLATION, C1, f'{key} is not configured')
            continue
        matches = glob_cert_store(path) if '*' in path else [path]
        if '*' in path:
            if matches:
                report.line(PASS, C1, f'{key} glob matches {len(matches)} file(s)')
            else:
                report.line(VIOLATION, C1,
                            f'{key} = {raw[key]!r} matches no certificates; '
                            'FSCertStore will trust nobody')
        elif os.path.isfile(path):
            report.line(PASS, C1, f'{key} exists ({os.path.basename(path)})')
        else:
            report.line(VIOLATION, C1, f'{key} points at a missing file: {raw[key]!r}')

    # The trust store must actually contain the allowed issuer's certificate.
    store_pattern = settings.get('security_cert_store')
    own_cert = settings.get('security_certificate')
    if store_pattern and own_cert:
        store_certs = glob_cert_store(store_pattern)
        with open(own_cert, 'rb') as handle:
            own_pem = handle.read()
        trusted = any(open(path, 'rb').read() == own_pem for path in store_certs)
        if trusted:
            report.line(PASS, C1,
                        'allowed issuer certificate is present in the trust store')
        else:
            report.line(VIOLATION, C1,
                        'allowed issuer certificate is NOT in the trust store; '
                        'every signed task fails verification (fail-closed)')

    report.line(RUNTIME, C1,
                'signature acceptance/rejection over the transport is proven by '
                'runtime_checks.py, not inferable from file presence')
    report.line(RUNTIME, C1,
                'certificate expiry, key/cert cryptographic match, and filesystem '
                'permissions are operational checks, not settled here')

    # ---- C2: JSON-only result handling -----------------------------------
    if settings.get('result_serializer') == 'json':
        report.line(PASS, C2, "result_serializer == 'json' (results are written as JSON)")
    else:
        report.line(VIOLATION, C2,
                    f"result_serializer = {settings.get('result_serializer')!r}")

    if settings.get('result_accept_content') == ['json']:
        report.line(PASS, C2,
                    "result_accept_content == ['json'] (kombu loads() rejects other "
                    'content types with ContentDisallowed)')
    else:
        report.line(VIOLATION, C2,
                    f'result_accept_content = '
                    f'{raw.get("result_accept_content")!r}; no explicit JSON read gate. '
                    'task_serializer=auth does not protect the result path')

    report.line(RUNTIME, C2,
                'the pickle-injection block is demonstrated by runtime_checks.py '
                'against cache+memory://')
    report.line(RUNTIME, C2,
                'write access to the production result store must be restricted at '
                'the infrastructure layer; config cannot enforce that')

    # ---- C3: event discipline --------------------------------------------
    report.line(RUNTIME, C3,
                'no Celery setting authenticates events: runtime_checks.py shows a '
                'forged task-succeeded event being accepted by the event pipeline')
    report.line(RUNTIME, C3,
                "application code must source completion from AsyncResult/result "
                'backend, never from event consumers')

    return report


def main() -> int:
    print('Static, offline review. Test certificates only; no services contacted.')
    summaries = []
    for name in CONFIGS:
        env = {'STORE_VARIANT': 'empty'} if name == 'incomplete_trust_counterexample' else None
        report = check_config(name, env)
        summaries.append((name, report))

    print('\n=== summary ===')
    for name, report in summaries:
        print(f'  {name}: {report.violations} violation(s), '
              f'{report.runtime_items} runtime-only item(s), '
              f'{report.passes} static pass(es)')
    print('\nNOTE: a violation-free static report is necessary, not sufficient. '
          'Run runtime_checks.py and review the operational items before claiming '
          'the deployment is secure.')
    return 1 if any(r.violations for _, r in summaries) else 0


if __name__ == '__main__':
    raise SystemExit(main())
