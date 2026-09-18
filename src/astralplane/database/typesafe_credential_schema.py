"""089.001 per-user TypeSafe credential and data-sharing acknowledgment.

``user_typesafe_credential`` stores one owner's TypeSafe System One key as
ciphertext only. AstralPlane never sees the plaintext: AstralDeep encrypts with
the credential key it already owns and hands Plane an opaque Fernet token. The
row also carries a short ``key_fingerprint`` -- ``sha256(key)[:12]`` -- which
exists so a late verification outcome from a replaced key cannot mark the
current key rejected, and so re-saving the same key can clear a rejected
status. The fingerprint is never displayed and never logged beside a user
identifier.

There is deliberately **no** ``system_typesafe_credential`` counterpart. A
deployment-wide TypeSafe key cannot be represented, which is what makes
"bring your own key" enforceable rather than advisory.

``user_data_sharing_acknowledgment`` records that an owner accepted the
third-party data-sharing notice before any credential was saved. It stores the
notice version and both the first and the latest acknowledgment time, so an
audit can correlate a save with the exact wording that was shown.

Both tables are additive and stand alone. Neither has a foreign key into
``user_llm_config``: clearing an LLM configuration never deletes a TypeSafe
credential or an acknowledgment, and clearing a TypeSafe credential never
touches the other two. Rollback drops both tables and restores the 088.008
registry marker; no other data is affected.
"""

TYPESAFE_CREDENTIAL_SCHEMA_STATEMENTS = (
    """
CREATE TABLE user_typesafe_credential (
    user_id TEXT PRIMARY KEY CHECK(char_length(user_id) BETWEEN 1 AND 512),
    api_key_enc BYTEA NOT NULL,
    key_fingerprint TEXT NOT NULL CHECK(key_fingerprint ~ '^[0-9a-f]{12}$'),
    last_verified_at TIMESTAMPTZ,
    last_verification_outcome TEXT NOT NULL DEFAULT 'unverified'
        CHECK(last_verification_outcome IN ('unverified','valid','rejected','unavailable')),
    last_outcome_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
)
""".strip(),
    """
CREATE TABLE user_data_sharing_acknowledgment (
    user_id TEXT PRIMARY KEY CHECK(char_length(user_id) BETWEEN 1 AND 512),
    notice_version TEXT NOT NULL CHECK(char_length(notice_version) BETWEEN 1 AND 64),
    acknowledged_at TIMESTAMPTZ NOT NULL,
    first_acknowledged_at TIMESTAMPTZ NOT NULL,
    CONSTRAINT user_data_sharing_acknowledgment_order
        CHECK(acknowledged_at >= first_acknowledged_at)
)
""".strip(),
)
