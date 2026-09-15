# Reviewed observations, 15 September 2026

These are selected records from controlled real demos, reviewed before commit.
Process source: `.nema/zygon/demos/c393f1ac/registry.sqlite`; browser source:
`.nema/zygon/b-31f721/registry/registry.sqlite` in the recovery checkout.

Sanitization is selection: only known demo byte chunks and the two Flix
invocation transition chains are included. Authentication, bootstrap state,
profiles, private wire frames, environment and unrelated traffic are excluded.
Record IDs, producer sequences, parent links, field values and available bytes
inside these selected records are unchanged. This is not a complete trace;
sequence gaps here reflect filtering and are not evidence of capture loss.

The two process stdout chunks concatenate to `birth:partial` followed by bytes
`ff fe 0a`. Their hashes are recorded independently. Each invocation preserves
its original correlation and accepted → started → completed parent chain.
The terminal record IDs match the actual Flix and terminal output reported in
`docs/acceptance.md`. No registered browser target remains live by virtue of
appearing in this fixture.
