"""HMAC-SHA256 signature verification over the RAW webhook body.

Verify BEFORE parsing JSON, always -- SPEC.md section 7. Parsing
untrusted bytes into a Python object before authenticating them means
whatever the JSON decoder does (allocate, recurse, raise) happens on
attacker-controlled input pre-authentication; verifying the raw bytes
first means a forged or corrupted request never reaches the parser at
all.

Signing over the RAW bytes specifically -- not a re-serialization of the
parsed body -- is also why this must run before parsing: two JSON
payloads that decode to the same object (different key order, different
whitespace) are DIFFERENT byte strings, and must produce DIFFERENT
signatures. Verifying against a re-encoded body would accept a payload
the provider never actually signed, as long as it happened to parse to
the same structure -- see tests/integration/test_webhooks.py's own test
for this exact case (item 6j).
"""

from __future__ import annotations

import hashlib
import hmac


def sign(raw_body: bytes, secret: str) -> str:
    """What the (mocked) provider itself would compute -- exposed here so
    the mock provider / load-test harness / tests can produce a valid
    signature without duplicating the HMAC construction.
    """
    return hmac.new(secret.encode(), raw_body, hashlib.sha256).hexdigest()


def verify_signature(raw_body: bytes, signature: str | None, secret: str) -> bool:
    """Constant-time comparison (hmac.compare_digest) -- a naive `==`
    leaks timing information proportional to how many leading bytes
    match, which is exactly the side channel an attacker forging a
    signature byte-by-byte would exploit.
    """
    if not signature:
        return False
    expected = sign(raw_body, secret)
    return hmac.compare_digest(expected, signature)
