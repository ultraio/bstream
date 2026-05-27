package blockstream

import (
	"bufio"
	"encoding/json"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// goldenEntry is the (step, block_num, cursor) tuple we extract per response.
// Per design §M8 (golden = behavior-equivalence, not byte-equivalence) and
// per the v3.2-minimal handoff's Reviewer-B trigger, we do NOT proto.Equal
// the full block — additive proto changes would break that.
type goldenEntry struct {
	Step     string
	BlockNum uint64
	Cursor   string
}

// loadGoldenJSONL parses the production-captured JSONL file. Each line is a
// compact pbbstream.BlockResponseV2 in protojson form (the firehose-CD captures
// it via grpcurl reflection; see HF1_implementation.md §Task 4).
//
// We parse minimal fields rather than protojson.Unmarshal because:
// (1) the Block payload uses Any with the dfuse-eosio codec type-URL — those
//     descriptors don't ship in this repo (the design holds bstream consumer-
//     agnostic).
// (2) (step, block_num, cursor) is all we need for behavior equivalence.
func loadGoldenJSONL(t *testing.T, path string) []goldenEntry {
	t.Helper()
	f, err := os.Open(path)
	require.NoError(t, err)
	defer f.Close()

	var out []goldenEntry
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 64*1024), 16*1024*1024) // blocks can be large
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		var raw map[string]json.RawMessage
		require.NoError(t, json.Unmarshal([]byte(line), &raw))

		var step string
		if v, ok := raw["step"]; ok {
			require.NoError(t, json.Unmarshal(v, &step))
		}
		var cursor string
		if v, ok := raw["cursor"]; ok {
			require.NoError(t, json.Unmarshal(v, &cursor))
		}

		var blockNum uint64
		if v, ok := raw["block"]; ok {
			var blockObj map[string]json.RawMessage
			require.NoError(t, json.Unmarshal(v, &blockObj))
			if numRaw, ok := blockObj["number"]; ok {
				// protojson emits uint64 as string OR as bare number — accept either
				numStr := strings.Trim(string(numRaw), `"`)
				parsed, err := strconv.ParseUint(numStr, 10, 64)
				require.NoError(t, err)
				blockNum = parsed
			}
		}

		out = append(out, goldenEntry{Step: step, BlockNum: blockNum, Cursor: cursor})
	}
	require.NoError(t, scanner.Err())
	return out
}

// TestGoldenFile_StructuralInvariants validates the captured production file
// without re-running it through a server (the design accepts this scope:
// behavior-equivalence is established once we know (a) every entry parses as
// valid pbbstream.BlockResponseV2-shaped data, (b) the (step, block_num)
// sequence is gap-free monotone IRREVERSIBLE, (c) cursors are non-empty
// opaque strings).
//
// Re-running the captured request through an in-process Server that reads
// from a synthetic blocks store would require shipping the underlying block
// payload too (~few MB of dfuse-eosio codec data). That is out of HF1 scope.
// The dummy-deploy soak (Task 8) covers behavior equivalence at the
// integration level by running the patched firehose against real prod data.
func TestGoldenFile_StructuralInvariants(t *testing.T) {
	path := "testdata/golden-100blocks.jsonl"
	entries := loadGoldenJSONL(t, path)
	require.GreaterOrEqual(t, len(entries), 100, "expected at least 100 captured blocks")
	require.LessOrEqual(t, len(entries), 102, "expected at most 102 (inclusive boundaries)")

	for i, e := range entries {
		require.Equal(t, "STEP_IRREVERSIBLE", e.Step,
			"entry %d: capture was --fork-steps=[STEP_IRREVERSIBLE]; mismatched step indicates a corrupt golden file", i)
		require.NotEmpty(t, e.Cursor, "entry %d: cursor must be present (post-fix every response carries an opaque cursor)", i)
		if i > 0 {
			require.Equal(t, entries[i-1].BlockNum+1, e.BlockNum,
				"entry %d: irreversible stream must be gap-free monotone (prev=%d cur=%d)",
				i, entries[i-1].BlockNum, e.BlockNum)
		}
	}
}

// TestGoldenFile_CursorUniqueness asserts cursor opaque encoding uniqueness
// across the captured sequence. Each STEP_IRREVERSIBLE response must carry a
// distinct cursor (different block IDs ⇒ different encoded payloads).
//
// Note: this test asserts capture-file self-consistency, not server-replay
// against captured tuples. A true behavior-equivalence-by-replay test is
// tracked as DEFER-HF1-N1 in ROADMAP.md — the dummy-deploy soak (HF1 §Task 8)
// covers replay at the integration level for HF1; a unit-test replay against
// a header-only mock blocks store will be added before HF2 production rollout.
func TestGoldenFile_CursorUniqueness(t *testing.T) {
	entries := loadGoldenJSONL(t, "testdata/golden-100blocks.jsonl")
	require.NotEmpty(t, entries)

	seen := make(map[string]int, len(entries))
	for i, e := range entries {
		if dup, ok := seen[e.Cursor]; ok {
			t.Fatalf("cursor not unique: entry %d duplicates entry %d (cursor=%q)", i, dup, e.Cursor)
		}
		seen[e.Cursor] = i
	}
}
