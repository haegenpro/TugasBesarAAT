# P3 retained-input replay demonstration

The browser recovery page is `/ops` on Client API. Sign in with the private
Internal Ops identity, `internal-demo`. Authentication is enforced by the server;
Media and Field identities cannot list, download, replay, or change reader policy.

Prepare the running application and open the browser endpoint:

```bash
python3 scripts/m2/access.py client-api
```

The example is a deliberate incompatible PVMBG source-contract change, followed
by adapting the reader and reprocessing the original retained bytes.

1. On `/ops`, select **BMKG / PVMBG** as owner and **PVMBG** as source. Set the
   reader policy to **Version 1** and activate it. Confirm the application is
   processing the normal source before introducing the fault.
2. From another terminal, run:

   ```bash
   python3 scripts/m2/mock_schema.py breaking
   ```

   The source renames `alert_level` to `level`, changes `eruption_count_24h` from
   an integer to text, and omits `ash_column_height_m`. It emits a new immutable
   report so that the changed contract becomes visible on the history stream.
3. Refresh the retained-message list. Show the parked state and failure reason.
   Download an original payload before replay; the source speaks Avro, so the
   file is binary. Preserve the file and its SHA-256 for evidence.
4. Activate reader **Version 2**. Select the retained PVMBG messages and press
   **Replay selected**. Refresh until those entries leave the unprocessed list.
   Check canonical results and stable event IDs to establish recovery without
   duplicate events. A queued replay alone does not establish completion.
5. Confirm that another attempt to process the same original report preserves
   its canonical identity. Keep the before/after evidence alongside the video.
6. Restore the source contract:

   ```bash
   python3 scripts/m2/mock_schema.py v1
   ```

   Leave reader 2 active until all broken-writer input is resolved. Reader 2 also
   reads the original v1 writer. After confirming no retained incompatible input
   remains, reader 1 can be restored for another demonstration.

`compatible` selects the optional `confidence_level` field. An empty-body
`POST /admin/schema-version` still toggles that original M1 behavior. Explicit
JSON mode selection is available for `v1`, `compatible`, and `breaking`; unknown
modes are rejected. The breaking writer uses separate major-contract registry
subjects, preserving FULL_TRANSITIVE on the original subjects. Its writer schema
is valid; the intentional failure is incompatibility with the selected reader.

This page demonstrates the M2 source-input quarantine. JetStream consumer DLQs
also retain delivery failures, but arbitrary corrupt bytes cannot become valid
events merely by requesting replay. Resolve the failure's cause first.

The deployed Client API flow was exercised on 2026-11-21: a 541-byte incompatible
PVMBG history payload was parked, its download matched the advertised SHA-256,
Media/Field identities received 403 for inspection and replay, and reader 2
resolved it. The first replay request selected one input; an immediate duplicate
request selected zero. The emitted demo report had exactly one canonical event.
Source v1 was restored afterward. This exercised the browser's HTTP endpoints
and served UI assets; recording and browser-click rehearsal are still separate.

Satellite replay shares the existing raw stream and can wait behind a large
backlog. For a short video use the controlled PVMBG scenario above. Queueing a
satellite replay does not prove it has completed.
