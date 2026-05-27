# blockstream/v2 testdata

## `golden-100blocks.jsonl`

101 captured `dfuse.bstream.v1.BlockResponseV2` messages, one compact JSON
object per line. Used by `golden_test.go` to validate the structural
invariants and cursor-uniqueness contract that HF1 (v3.2-minimal Phase 2a)
preserves across the gateCursor bug-class deletion.

### Provenance

| Field | Value |
|---|---|
| Source pod | `ultra-prod-mainnet/firehose-v3-0` (saas-europe-west1) |
| Capture date | 2026-05-27 (~16:07 UTC) |
| Production head at capture | 311426832 |
| Captured range (inclusive) | 311425732 .. 311425832 (101 blocks, 1000 behind head) |
| Fork-steps filter | `STEP_IRREVERSIBLE` only |
| Capture command | `kubectl port-forward` + `grpcurl` via server reflection, post-processed to one compact JSON object per line |

### Why this range?

`head - 1000` is ~16 minutes behind chain tip on Spring/Savanna (~1s blocks).
That's >> LIB-distance (2 blocks on Savanna 1s-finality), so every block is
provably finalized and cannot fork. The 100-block span is small enough to
review by hand if needed (~750 KiB compacted) and large enough to cover a
realistic burst of cursor traffic.

### Recapture procedure

```bash
kubectl port-forward -n ultra-prod-mainnet firehose-v3-0 9000:9000 &
PF=$!
sleep 3

HEAD=$(kubectl exec -n ultra-prod-mainnet firehose-v3-0 -- \
  curl -s localhost:9102/metrics | grep '^head_block_number{' | \
  awk '{printf "%.0f", $2}')
START=$((HEAD - 1100))
END=$((HEAD - 1000))

grpcurl -insecure -max-msg-sz 33554432 \
  -d "{\"start_block_num\": $START, \"stop_block_num\": $END, \"fork_steps\": [\"STEP_IRREVERSIBLE\"]}" \
  localhost:9000 dfuse.bstream.v1.BlockStreamV2/Blocks \
  | python3 -c '
import sys, json
buf = ""; depth = 0; in_string = False; escape = False
with open("golden-100blocks.jsonl", "w") as out:
    for ch in sys.stdin.read():
        buf += ch
        if escape: escape = False; continue
        if ch == "\\": escape = True; continue
        if ch == "\"": in_string = not in_string; continue
        if in_string: continue
        if ch == "{": depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                out.write(json.dumps(json.loads(buf.strip()), separators=(",", ":")) + "\n")
                buf = ""
'

kill $PF
```

The firehose serves TLS (use `-insecure`), exposes gRPC server reflection,
and supports a 32 MiB max-msg-sz (block payloads can be ~150 KiB).

### Schema

Each line decodes to a `dfuse.bstream.v1.BlockResponseV2`:

```json
{
  "block": {
    "@type": "type.googleapis.com/dfuse.eosio.codec.v1.Block",
    "number": <uint64>,
    "id": "<hex>",
    ...
  },
  "step": "STEP_IRREVERSIBLE",
  "cursor": "<opaque-base64>"
}
```

The `block` payload uses `Any` with the dfuse-eosio codec type URL. The
golden test parses only `step`, `block.number`, and `cursor` — it does not
unmarshal the full block (the design's M8 calls for tuple-equivalence, not
byte-equivalence).
