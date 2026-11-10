"""Counterexample A: task serializer hardened, result side left open.

The common mistake is assuming task_serializer='auth' also protects results.
It does not: the auth serializer only wraps *task messages*. Result records
are written and read through a separate path (celery/backends/base.py) with
its own serializer and accept list. Here the result accept gate is missing,
so a party that can write to the result store can inject a pickle-encoded
result that the backend will deserialize.

Violations:
  C2 - result_accept_content is not pinned to JSON.
"""

broker_url = 'memory://'
result_backend = 'cache+memory://'

# C1 is satisfied on the task path.
task_serializer = 'auth'
accept_content = ['auth']
security_digest = 'sha256'
security_key = 'certs/issuer-allowed.key'
security_certificate = 'certs/issuer-allowed.crt'
security_cert_store = 'certs/trusted/*.pem'

# C2 fails: we write JSON, but never restrict what we are willing to read.
result_serializer = 'json'
# result_accept_content intentionally unset. The backend then falls back to
# accept_content for its read gate; because that list only names the auth task
# envelope ('application/data'), result records have no JSON-specific gate at
# all - a service that later enables pickle (e.g. a shared store or a
# migration) inherits no protection from task_serializer='auth'. The runtime
# script demonstrates the open read path with an explicit pickle allowlist.

event_serializer = 'json'
