package blockstream

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/streamingfast/bstream"
	"github.com/streamingfast/bstream/forkable"
	"github.com/streamingfast/dstore"
	pbbstream "github.com/streamingfast/pbgo/dfuse/bstream/v1"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

// Closes DEFER-HF1-N1 (ROADMAP entry, v3.2-minimal).
//
// Builds a header-only synthetic block chain covering bundles 0311425700 +
// 0311425800 (the same range the production golden file captured), drives it
// through an in-process patched Server via BlocksFromLocal, and asserts the
// emitted (Step, BlockNum) sequence matches the captured golden entry-for-entry.
//
// Why this exists: the golden_test.go pair asserts capture-file self-
// consistency only (gap-free monotone IRREVERSIBLE + cursor uniqueness).
// Reviewer-B's P1-B1 noted that a true replay test — feeding identical input
// through the patched Server and comparing output — is the unit-level
// regression gate complementing Task 8's 24h integration soak. This test is
// that unit-level gate; the soak is the integration-level gate.
//
// Why we cannot byte-match cursors against golden: production cursors encode
// production block IDs (Block.ID / HeadBlock.ID / LIB.ID) that we do not
// possess outside the encrypted opaque payload — and reconstructing the
// production chain's exact LIB-advance sequence would require the underlying
// EOSIO codec data the bstream test harness is intentionally agnostic to (per
// design M8: behavior-equivalence, not byte-equivalence). What we DO match:
//   - (Step, BlockNum) tuple equality with golden entries[0..100]
//   - Every emitted cursor decodes round-trip to a Cursor{Step=Irreversible}
//     whose Block.Num equals the response's block num
//   - Cursors are unique within the test run
//   - The output sequence is gap-free monotone, the same invariant Reviewer-B
//     cared about
//
// Chain construction note: each synthetic block has libnum=N-1. The chain
// extends into a second bundle file past the requested stop so FileSource has
// material to keep streaming until the outer stop-block check terminates the
// source. The patched Server's emit ordering matches the production capture
// for this 1-block LIB-lag input: 101 IRREVERSIBLE responses, last one for
// 311425832 (= request stop), matching golden entry-for-entry.
const (
	replayFirstFileBase  = uint64(311_425_700)
	replaySecondFileBase = uint64(311_425_800)
	replayChainStart     = uint64(311_425_700)
	replayChainEnd       = uint64(311_425_899) // fill both bundle files
	replayRequestStart   = int64(311_425_732)
	replayRequestStop    = uint64(311_425_832) // matches golden capture stop
	replayGoldenFirst    = uint64(311_425_732)
	replayGoldenLast     = uint64(311_425_832)
)

// syntheticReplayID returns a 64-char hex ID for a given block num where the
// first 8 hex chars encode the block num (bstream.blocknum reads only those 8
// chars to recover the number for test JSON blocks — see testing.go).
func syntheticReplayID(num uint64) string {
	// %08x big-endian uint32 of block num (caps blocks at uint32 range, ample
	// for 311M-block test).
	return fmt.Sprintf("%08x", uint32(num)) + strings.Repeat("0", 56)
}

// buildSyntheticBundle returns a JSONL string containing one TestBlockReader-
// compatible block per line for [from, to] inclusive. Each block has
// libnum = num - 1; block num 0 is skipped (libnum underflow guard, irrelevant
// for our range).
func buildSyntheticBundle(from, to uint64) string {
	var b strings.Builder
	for num := from; num <= to; num++ {
		id := syntheticReplayID(num)
		prevID := syntheticReplayID(num - 1)
		libNum := num - 1
		// TestBlockFromJSON honors {"id","prev","libnum"}; block num is derived
		// from the first 8 hex chars of id (see bstream.blocknum).
		b.WriteString(fmt.Sprintf(`{"id":%q,"prev":%q,"libnum":%d}`, id, prevID, libNum))
		b.WriteByte('\n')
	}
	return b.String()
}

func TestServer_Blocks_GoldenReplay(t *testing.T) {
	// 1. Load golden expectations and trim to the inclusive [first, last] range
	//    we expect to see — the synthetic replay does not exceed this range.
	golden := loadGoldenJSONL(t, "testdata/golden-100blocks.jsonl")
	require.GreaterOrEqual(t, len(golden), 101, "golden capture must hold ≥101 entries; see testdata/README.md")

	// 2. Install the test block decoder so forkable receives populated
	//    pbbstream.Block values. The pattern mirrors TestLocalBlocks in
	//    stream_blocks_test.go.
	bstream.GetBlockDecoder = bstream.BlockDecoderFunc(func(blk *bstream.Block) (interface{}, error) {
		block := new(pbbstream.Block)
		block.Number = blk.Number
		block.Id = blk.Id
		block.PreviousId = blk.PreviousId
		return block, nil
	})

	// 3. Wire a mock blocks store with two 100-block bundles. FileSource reads
	//    file "0311425700" then "0311425800" (per filesource.go runMergeFile
	//    naming convention: %010d of the bundle base).
	store := dstore.NewMockStore(nil)
	store.SetFile(
		fmt.Sprintf("%010d", replayFirstFileBase),
		[]byte(buildSyntheticBundle(replayChainStart, replayFirstFileBase+99)),
	)
	store.SetFile(
		fmt.Sprintf("%010d", replaySecondFileBase),
		[]byte(buildSyntheticBundle(replaySecondFileBase, replayChainEnd)),
	)

	// 4. Spin up the patched Server. nil live source (file-only replay) + nil
	//    tracker (matches the no-Tracker path in firehose.New that uses
	//    fileStartBlock = uint64(startBlockNum) directly). resolveHeadInfo
	//    degrades to a zero HeadInfo with nil tracker (per server.go finding
	//    P2-A3); the classifier then skips bounds checks — which is the right
	//    behavior for this offline replay (no live chain to compare against).
	s := NewServer(
		zap.NewNop(),
		[]dstore.Store{store},
		nil,
		nil,
		nil,
		nil,
	)

	// 5. Issue the request. Stop = goldenLast+1 to give forkable's LIB
	//    advance the lookahead it needs to emit IRREVERSIBLE for goldenLast
	//    (see file-level comment).
	client := s.BlocksFromLocal(context.Background(), &pbbstream.BlocksRequestV2{
		StartBlockNum: replayRequestStart,
		StopBlockNum:  replayRequestStop,
		ForkSteps:     []pbbstream.ForkStep{pbbstream.ForkStep_STEP_IRREVERSIBLE},
	})

	// 6. Collect responses. We cap at 200 to avoid wedging the test if the
	//    pipeline somehow keeps producing (which would itself be a regression
	//    surfaced as a test failure).
	type emitted struct {
		Step     string
		BlockNum uint64
		Cursor   string
	}
	var got []emitted
	for i := 0; i < 200; i++ {
		resp, err := client.Recv()
		if resp == nil {
			// Pipe closed (stop reached or context done).
			require.NoError(t, err, "pipe closed with error after %d responses", len(got))
			break
		}
		require.NoError(t, err)
		// Decode the cursor to extract the trusted block number — resp.Block is
		// a *pbany.Any wrapping the codec-specific payload, and decoding it
		// would re-introduce the codec coupling the design intentionally avoids.
		cur, err := forkable.CursorFromOpaque(resp.Cursor)
		require.NoError(t, err, "emitted cursor at i=%d does not round-trip", i)
		require.Equal(t, forkable.StepIrreversible, cur.Step,
			"emitted cursor.Step at i=%d must be Irreversible (request filtered to IRREVERSIBLE only)", i)
		got = append(got, emitted{
			Step:     resp.Step.String(),
			BlockNum: cur.Block.Num(),
			Cursor:   resp.Cursor,
		})
	}

	// 7. Pairwise diff against golden's [first, last] inclusive range. Locate
	//    the golden slice that covers replayGoldenFirst..replayGoldenLast (in
	//    the current testdata file, that's entries[0..100], but anchor by
	//    block_num rather than index for safety against future re-captures).
	var expected []emitted
	for _, ge := range golden {
		if ge.BlockNum < replayGoldenFirst || ge.BlockNum > replayGoldenLast {
			continue
		}
		expected = append(expected, emitted{Step: ge.Step, BlockNum: ge.BlockNum, Cursor: ge.Cursor})
	}
	require.Equal(t, 101, len(expected),
		"golden slice [%d..%d] expected to hold exactly 101 entries; got %d",
		replayGoldenFirst, replayGoldenLast, len(expected))

	require.Equal(t, len(expected), len(got),
		"replay length mismatch: expected %d IRREVERSIBLE responses, got %d", len(expected), len(got))

	for i := range expected {
		require.Equal(t, expected[i].Step, got[i].Step,
			"step mismatch at i=%d (block %d)", i, expected[i].BlockNum)
		require.Equal(t, expected[i].BlockNum, got[i].BlockNum,
			"block_num mismatch at i=%d", i)
		require.NotEmpty(t, got[i].Cursor, "emitted cursor at i=%d must not be empty", i)
		// We do NOT assert byte-equality of got[i].Cursor vs expected[i].Cursor
		// (see file-level comment). What we assert: golden's cursor decodes to
		// the same (Step, BlockNum) the emitted cursor decoded to.
		goldenCur, err := forkable.CursorFromOpaque(expected[i].Cursor)
		require.NoError(t, err)
		require.Equal(t, forkable.StepIrreversible, goldenCur.Step,
			"decoded golden step at i=%d must be Irreversible", i)
		require.Equal(t, goldenCur.Block.Num(), got[i].BlockNum,
			"decoded golden block_num at i=%d mismatches emitted block_num", i)
	}

	// 8. Cursor uniqueness within the emitted run — the same invariant
	//    TestGoldenFile_CursorUniqueness asserts on golden.
	seen := make(map[string]int, len(got))
	for i, e := range got {
		if dup, ok := seen[e.Cursor]; ok {
			t.Fatalf("emitted cursor not unique: entry %d duplicates entry %d (cursor=%q, block_num=%d)",
				i, dup, e.Cursor, e.BlockNum)
		}
		seen[e.Cursor] = i
	}
}
