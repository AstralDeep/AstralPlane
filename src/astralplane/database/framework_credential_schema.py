"""088.008 owner-issued framework credentials and finite offline-grant allowance.

``framework_credential`` stores hash-only bearer tokens an owner mints for an
external caller (an SDK, MCP or A2A client) while an interactive session (or a
native client's own credential) is live. Plane never sees or persists the
plaintext token; only its SHA-256 hex digest and a short display prefix are
stored. A credential is bound to the exact issuer reference (a session
incarnation or native credential id) that was live when it was minted, and to
a closed scope set; it never widens itself and never binds to an untrusted
delegation chain.

The additive nullable ``user_offline_grant`` columns give a scheduled grant a
finite admission allowance (``max_admissions``); a NULL value keeps every
pre-088.008 grant's unlimited-admissions semantics unchanged.

Both changes are additive: no existing row, column default, or constraint is
altered, and neither table participates in the fresh-install fast path.
"""

FRAMEWORK_CREDENTIAL_SCHEMA_STATEMENTS = (
    """
CREATE TABLE framework_credential (
    id UUID PRIMARY KEY,
    owner_id TEXT NOT NULL CHECK(char_length(owner_id) BETWEEN 1 AND 512),
    name TEXT NOT NULL CHECK(char_length(name) BETWEEN 1 AND 256),
    scopes JSONB NOT NULL
        CHECK(jsonb_typeof(scopes)='array' AND octet_length(scopes::text)<=4096),
    token_hash TEXT NOT NULL UNIQUE CHECK(token_hash ~ '^[0-9a-f]{64}$'),
    token_prefix TEXT NOT NULL CHECK(char_length(token_prefix) BETWEEN 1 AND 16),
    issuer_kind TEXT NOT NULL CHECK(issuer_kind IN ('session_incarnation','native_credential')),
    issuer_reference TEXT NOT NULL CHECK(char_length(issuer_reference) BETWEEN 1 AND 256),
    max_admissions INTEGER NOT NULL CHECK(max_admissions BETWEEN 1 AND 10000),
    consumed_admissions INTEGER NOT NULL DEFAULT 0 CHECK(consumed_admissions >= 0),
    created_at TIMESTAMPTZ NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL,
    revoked_at TIMESTAMPTZ,
    last_used_at TIMESTAMPTZ,
    CONSTRAINT framework_credential_allowance CHECK(consumed_admissions <= max_admissions),
    CONSTRAINT framework_credential_lifetime CHECK(expires_at > created_at)
)
""".strip(),
    "CREATE INDEX framework_credential_owner ON framework_credential (owner_id, revoked_at)",
    "ALTER TABLE user_offline_grant ADD COLUMN max_admissions INTEGER "
    "CHECK(max_admissions IS NULL OR max_admissions BETWEEN 1 AND 10000)",
    "ALTER TABLE user_offline_grant ADD COLUMN consumed_admissions INTEGER "
    "CHECK(consumed_admissions IS NULL OR consumed_admissions >= 0)",
    "ALTER TABLE user_offline_grant ADD CONSTRAINT user_offline_grant_allowance CHECK("
    "max_admissions IS NULL OR consumed_admissions IS NOT NULL"
    " AND consumed_admissions <= max_admissions)",
)
