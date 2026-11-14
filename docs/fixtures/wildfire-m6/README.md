# Durable wildfire fixtures

All inputs and provinces here are synthetic. They are not evaluator data or a
specification of the live satellite feed.

- `config.json` configures the SQL processor with explicit execution budgets.
  Its province path points to the unchanged reference fixture table.
- `reference-config.json` selects reference algorithm 2 for the same namespace,
  initial offset, lateness and province digest.
- `mixed.expected.json` independently specifies the eighth operation of
  `../wildfire-m5/mixed-merge-split.jsonl`: AB retains the identity born at offset
  2; CDEFG retains the identity born at offset 7. Identity SHA-256 values were
  calculated from the compact UTF-8 derivation tuples with Python hashlib.
  Equatorial equal-weight centers are analytical means of the member
  longitudes; numeric tolerances apply to reported centers only.

The actual-storage tests additionally reuse the unchanged bridge replay and
every shared historical-restoration contract case. Abstract historical labels
are seeded at the graph/lineage boundary, with valid derivation bindings; they
are explicitly not presented as parsed live detections.

The large-output test seeds synthetic logical intents at the output boundary
to exercise a complete prepared reference larger than 1 MiB. It establishes
neither large-graph throughput nor realistic satellite-name lengths.
