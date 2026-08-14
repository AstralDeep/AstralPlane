# Synthetic pre-split fixture

This fixture describes a small `066.001` AstralDeep durable-state snapshot. Every identifier and
payload is synthetic and contains no PHI, credential, token, real user content, or host path. The
current tests validate its structure, coverage, relative blob locator, and SHA-256 binding. An
executable PostgreSQL loader and upgrade/recovery replay remain a separate integration checkpoint.
