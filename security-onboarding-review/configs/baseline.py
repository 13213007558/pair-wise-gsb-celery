"""Baseline Celery configuration satisfying all three review constraints.

Constraints:
  C1 Task messages are accepted only from the allowed signer.
  C2 Result backend data is accepted only as JSON.
  C3 Monitoring events are never treated as proof of task completion.

This file covers the static, file-based part of C1 and C2. C3 is a
consumer-side rule enforced in application code (see runtime_checks.py).
"""

# --- Transport: in-memory broker and result store, offline only -----------
broker_url = 'memory://'
result_backend = 'cache+memory://'

# --- C1: signed task messages, restricted to the allowed issuer -----------
# setup_security() requires exactly this combination and raises
# ImproperlyConfigured otherwise (celery/security/__init__.py).
task_serializer = 'auth'
accept_content = ['auth']
security_digest = 'sha256'
# Paths are resolved relative to the review root by the check scripts so the
# config stays portable. They point at test-only material.
security_key = 'certs/issuer-allowed.key'
security_certificate = 'certs/issuer-allowed.crt'
security_cert_store = 'certs/trusted/*.pem'

# --- C2: JSON-only results, pinned on both ends ---------------------------
# result_serializer picks what we write; result_accept_content is the gate
# kombu's loads() enforces on read (ContentDisallowed on mismatch).
result_serializer = 'json'
result_accept_content = ['json']

# --- Monitoring events: observability only, not a control plane -----------
event_serializer = 'json'
# No Celery setting can make events trustworthy. Completion must be read from
# the signed task message / result backend, never from a task-succeeded event.
