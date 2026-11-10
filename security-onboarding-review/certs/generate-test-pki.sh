#!/usr/bin/env bash
# Test-only PKI for the Celery onboarding review.
# No production material is used or contacted. Re-run to regenerate.
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
cd "$HERE"

# 1. Dedicated test CA (not trusted anywhere outside this review).
openssl req -x509 -newkey rsa:2048 -nodes -days 3650 \
  -keyout test-ca.key -out test-ca.crt \
  -subj "/C=XX/O=Celery Onboarding Review/CN=Test CA (DO NOT TRUST)" 2>/dev/null

make_leaf () {
  local name="$1" cn="$2"
  openssl genrsa -out "$name.key" 2048 2>/dev/null
  openssl req -new -key "$name.key" -out "$name.csr" -subj "/C=XX/O=Celery Onboarding Review/CN=$cn"
  openssl x509 -req -days 3650 -in "$name.csr" \
    -CA test-ca.crt -CAkey test-ca.key -CAcreateserial \
    -out "$name.crt" 2>/dev/null
  rm -f "$name.csr"
}

# 2. The single issuer allowed to sign task messages.
make_leaf issuer-allowed "Allowed Task Issuer"
# 3. A second, untrusted issuer used to prove the trust store rejects it.
make_leaf issuer-rogue "Rogue Task Issuer"

# 4. Trust stores used by the three configs.
cp issuer-allowed.crt trusted/issuer-allowed.pem
# wrong-store contains only the unrelated (rogue) issuer, never the allowed one.
cp issuer-rogue.crt wrong-store/issuer-rogue.pem
# empty-store stays empty on purpose.
rm -f empty-store/*.pem
rm -f test-ca.srl

echo "Generated test PKI in $HERE:"
find . -type f \( -name '*.crt' -o -name '*.key' -o -name '*.pem' \) | sort
