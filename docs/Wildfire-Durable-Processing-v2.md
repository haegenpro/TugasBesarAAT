# Wildfire durable processing contract v2

This addendum preserves the original wildfire agreement and reference version 1.
Version 2 explicitly resolves simultaneous splitting and merging:

1. Restore exact inactive historical memberships using the existing precedence
   and historical-match tie breakers.
2. Order unmatched qualifying children by earliest member acquisition time, then
   its exact UTF-8 key. Each active predecessor claims its earliest remaining
   child once. A predecessor identity can be assigned to at most one child.
3. Competing claims are ordered by the earliest surviving contributing member
   in that child, then its exact key. The winner retains its identity. Losers
   become inactive without a second claim. Unclaimed children get new identities.

In the eight-operation mixed reproduction, AB retains the old ABCDE identity;
CDEFG retains the old FG identity. Existing fire-v1 derivation is unchanged.
Reference version 1 continues to report the historical ambiguity.

Normalized satellite adapter v1 uses content type
`application/vnd.bnpb.satellite-fixture+json;version=1`. RawInput payload is the
complete original JSONL operation bytes, without the line delimiter. The nested
`payload` retains the independently supplied original bytes; detection values
are explicitly normalized fixture values, not an inferred live feed contract.
Namespace, offset, kind, and virtual_now must agree with RawInput metadata.

Durable work uses invisible versioned staging and bounded, fenced checkpoints.
Only the protected activation transaction advances prepared progress and exposes
the staged state/lineage and immutable logical intents together. Preparing a
logical intent does not imply canonical application. An executable request is
sealed once its predecessor canonical outcomes establish expected revisions.
The immutable logical intent and sealed request are separate retained records;
neither an ambiguous nor a committed canonical request is rewritten.

Semantic run configuration (namespace/start, algorithm, lateness, provinces) is
immutable. Execution quotas are explicit and may only change through a validated
restart. Exhaustion keeps obligations pending; it never evicts history or splits
the visible effects of a correction.
