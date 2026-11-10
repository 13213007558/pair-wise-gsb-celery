"""Counterexample B: signing configured, but the trust store is incomplete.

setup_security() only checks that the three file paths are non-empty
strings. FSCertStore globs the store pattern at registration time; a store
that contains no copy of the allowed issuer's certificate silently trusts
nobody, and the first signed task message fails at consumption with
SecurityError('Unknown certificate: ...'). Two broken variants are modeled
(select one with STORE_VARIANT):

  empty: the store directory contains no certificates at all.
  wrong: the store contains the rogue issuer, not the allowed one.

Violations:
  C1 - the allowed issuer's certificate is absent from the trust store, so
       tasks from the approved signer cannot be verified (deny-on-failure
       integrity/availability control, not silent acceptance).
"""

import os

STORE_VARIANT = os.environ.get('STORE_VARIANT', 'empty')
_STORES = {
    'empty': 'certs/empty-store/*.pem',
    'wrong': 'certs/wrong-store/*.pem',
}

broker_url = 'memory://'
result_backend = 'cache+memory://'

task_serializer = 'auth'
accept_content = ['auth']
security_digest = 'sha256'
security_key = 'certs/issuer-allowed.key'
security_certificate = 'certs/issuer-allowed.crt'
security_cert_store = _STORES[STORE_VARIANT]

# C2 is correctly configured in this variant to isolate the C1 failure.
result_serializer = 'json'
result_accept_content = ['json']

event_serializer = 'json'
