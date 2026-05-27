package blockstream

import (
	"strings"
	"testing"

	"github.com/streamingfast/bstream"
	"github.com/streamingfast/bstream/forkable"
	pbbstream "github.com/streamingfast/pbgo/dfuse/bstream/v1"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// makeCursor builds an opaque cursor string with the given step + block num.
// All three block IDs are synthetic-but-deterministic (sha-style hex of the num).
func makeCursor(t *testing.T, step forkable.StepType, blockNum uint64) string {
	t.Helper()
	cur := &forkable.Cursor{
		Step:      step,
		Block:     bstream.NewBlockRef(syntheticID(blockNum, 'a'), blockNum),
		HeadBlock: bstream.NewBlockRef(syntheticID(blockNum, 'a'), blockNum),
		LIB:       bstream.NewBlockRef(syntheticID(blockNum, 'a'), blockNum),
	}
	return cur.ToOpaque()
}

// syntheticID returns a deterministic 64-char hex id for tests.
func syntheticID(blockNum uint64, suffix byte) string {
	hexNum := []byte("0000000000000000")
	for i := 15; i >= 0 && blockNum > 0; i-- {
		v := byte(blockNum & 0xf)
		blockNum >>= 4
		if v < 10 {
			hexNum[i] = '0' + v
		} else {
			hexNum[i] = 'a' + v - 10
		}
	}
	return string(hexNum) + strings.Repeat(string(suffix), 48)
}

func mustStatus(t *testing.T, err error) (codes.Code, string) {
	t.Helper()
	st, ok := status.FromError(err)
	require.True(t, ok, "error is not a gRPC status: %v", err)
	return st.Code(), st.Message()
}

func TestClassifyCursor_NoCursor_StartBlockBelowHead_ReturnsStartBlock(t *testing.T) {
	req := &pbbstream.BlocksRequestV2{StartBlockNum: 500}
	got, err := classifyCursor(req, HubHeadInfo{HeadNum: 1000, LIBNum: 998})
	require.NoError(t, err)
	require.Equal(t, int64(500), got)
}

func TestClassifyCursor_NoCursor_StartBlockAtHead_ReturnsStartBlock(t *testing.T) {
	req := &pbbstream.BlocksRequestV2{StartBlockNum: 1000}
	got, err := classifyCursor(req, HubHeadInfo{HeadNum: 1000, LIBNum: 998})
	require.NoError(t, err)
	require.Equal(t, int64(1000), got)
}

func TestClassifyCursor_NoCursor_StartBlockAheadOfHead_ReturnsInvalidArgument(t *testing.T) {
	req := &pbbstream.BlocksRequestV2{StartBlockNum: 1001}
	_, err := classifyCursor(req, HubHeadInfo{HeadNum: 1000, LIBNum: 998})
	code, _ := mustStatus(t, err)
	require.Equal(t, codes.InvalidArgument, code)
}

func TestClassifyCursor_NoCursor_NegativeStartBlock_PreservesSentinel(t *testing.T) {
	// Negative StartBlockNum is the "relative to head" sentinel; firehose.New
	// + Tracker resolves it. Classifier must pass it through untouched.
	req := &pbbstream.BlocksRequestV2{StartBlockNum: -1}
	got, err := classifyCursor(req, HubHeadInfo{HeadNum: 1000, LIBNum: 998})
	require.NoError(t, err)
	require.Equal(t, int64(-1), got)
}

func TestClassifyCursor_NoCursor_HeadInfoUnavailable_SkipsBoundsCheck(t *testing.T) {
	// HeadNum=0 ⇒ Tracker getter not registered; preserve pre-classifier behavior
	// where unwired Trackers never gated requests on head.
	req := &pbbstream.BlocksRequestV2{StartBlockNum: 999999999}
	got, err := classifyCursor(req, HubHeadInfo{HeadNum: 0, LIBNum: 0})
	require.NoError(t, err)
	require.Equal(t, int64(999999999), got)
}

func TestClassifyCursor_InvalidCursorOpaque_ReturnsInvalidArgument(t *testing.T) {
	req := &pbbstream.BlocksRequestV2{StartCursor: "not-a-valid-cursor"}
	_, err := classifyCursor(req, HubHeadInfo{HeadNum: 1000, LIBNum: 998})
	code, _ := mustStatus(t, err)
	require.Equal(t, codes.InvalidArgument, code)
}

func TestClassifyCursor_IrreversibleCursor_BelowLIB_ReturnsBlockNumPlus1(t *testing.T) {
	req := &pbbstream.BlocksRequestV2{StartCursor: makeCursor(t, forkable.StepIrreversible, 800)}
	got, err := classifyCursor(req, HubHeadInfo{HeadNum: 1000, LIBNum: 998})
	require.NoError(t, err)
	require.Equal(t, int64(801), got)
}

func TestClassifyCursor_IrreversibleCursor_AtExactlyLIB_ReturnsBlockNumPlus1(t *testing.T) {
	req := &pbbstream.BlocksRequestV2{StartCursor: makeCursor(t, forkable.StepIrreversible, 998)}
	got, err := classifyCursor(req, HubHeadInfo{HeadNum: 1000, LIBNum: 998})
	require.NoError(t, err)
	require.Equal(t, int64(999), got)
}

func TestClassifyCursor_IrreversibleCursor_AboveLIB_ReturnsOutOfRange(t *testing.T) {
	req := &pbbstream.BlocksRequestV2{StartCursor: makeCursor(t, forkable.StepIrreversible, 999)}
	_, err := classifyCursor(req, HubHeadInfo{HeadNum: 1000, LIBNum: 998})
	code, _ := mustStatus(t, err)
	require.Equal(t, codes.OutOfRange, code)
}

func TestClassifyCursor_IrreversibleCursor_AheadOfHead_ReturnsInvalidArgument(t *testing.T) {
	req := &pbbstream.BlocksRequestV2{StartCursor: makeCursor(t, forkable.StepIrreversible, 1500)}
	_, err := classifyCursor(req, HubHeadInfo{HeadNum: 1000, LIBNum: 998})
	code, _ := mustStatus(t, err)
	require.Equal(t, codes.InvalidArgument, code)
}

func TestClassifyCursor_NewStepCursor_ReturnsUnimplemented(t *testing.T) {
	req := &pbbstream.BlocksRequestV2{StartCursor: makeCursor(t, forkable.StepNew, 800)}
	_, err := classifyCursor(req, HubHeadInfo{HeadNum: 1000, LIBNum: 998})
	code, _ := mustStatus(t, err)
	require.Equal(t, codes.Unimplemented, code)
}

func TestClassifyCursor_UndoStepCursor_ReturnsUnimplemented(t *testing.T) {
	req := &pbbstream.BlocksRequestV2{StartCursor: makeCursor(t, forkable.StepUndo, 800)}
	_, err := classifyCursor(req, HubHeadInfo{HeadNum: 1000, LIBNum: 998})
	code, _ := mustStatus(t, err)
	require.Equal(t, codes.Unimplemented, code)
}

func TestClassifyCursor_BoundaryTable(t *testing.T) {
	type tc struct {
		name      string
		head      uint64
		lib       uint64
		step      forkable.StepType // 0 = no cursor
		startBlk  int64             // used when no cursor
		cursorBlk uint64            // used when cursor
		wantStart int64
		wantCode  codes.Code // OK = success
	}

	cases := []tc{
		// no-cursor boundary table
		{"no-cursor head=0 lib=0 start=0", 0, 0, 0, 0, 0, 0, codes.OK},
		{"no-cursor head=0 lib=0 start=big", 0, 0, 0, 999999, 0, 999999, codes.OK},
		{"no-cursor head=10 start=0", 10, 5, 0, 0, 0, 0, codes.OK},
		{"no-cursor head=10 start=10", 10, 5, 0, 10, 0, 10, codes.OK},
		{"no-cursor head=10 start=11", 10, 5, 0, 11, 0, 0, codes.InvalidArgument},
		{"no-cursor head=10 start=-1 sentinel", 10, 5, 0, -1, 0, -1, codes.OK},
		{"no-cursor head=10 start=-99 sentinel", 10, 5, 0, -99, 0, -99, codes.OK},

		// irreversible cursor boundary table
		{"irr cursor=0 head=10 lib=5", 10, 5, forkable.StepIrreversible, 0, 0, 1, codes.OK},
		{"irr cursor=1 head=10 lib=5", 10, 5, forkable.StepIrreversible, 0, 1, 2, codes.OK},
		{"irr cursor=lib-1", 10, 5, forkable.StepIrreversible, 0, 4, 5, codes.OK},
		{"irr cursor=lib", 10, 5, forkable.StepIrreversible, 0, 5, 6, codes.OK},
		{"irr cursor=lib+1 OutOfRange", 10, 5, forkable.StepIrreversible, 0, 6, 0, codes.OutOfRange},
		{"irr cursor=head OutOfRange (because head>LIB)", 10, 5, forkable.StepIrreversible, 0, 10, 0, codes.OutOfRange},
		{"irr cursor=head+1 InvalidArgument", 10, 5, forkable.StepIrreversible, 0, 11, 0, codes.InvalidArgument},
		{"irr cursor head=lib (no rev distance), cursor=head", 10, 10, forkable.StepIrreversible, 0, 10, 11, codes.OK},
		{"irr cursor head=lib, cursor=head+1 InvalidArgument", 10, 10, forkable.StepIrreversible, 0, 11, 0, codes.InvalidArgument},
		{"irr cursor headInfo unavailable", 0, 0, forkable.StepIrreversible, 0, 100, 101, codes.OK},

		// reversible-step cursors
		{"new step cursor → Unimplemented", 10, 5, forkable.StepNew, 0, 7, 0, codes.Unimplemented},
		{"undo step cursor → Unimplemented", 10, 5, forkable.StepUndo, 0, 7, 0, codes.Unimplemented},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			req := &pbbstream.BlocksRequestV2{}
			if c.step == 0 {
				req.StartBlockNum = c.startBlk
			} else {
				req.StartCursor = makeCursor(t, c.step, c.cursorBlk)
			}

			got, err := classifyCursor(req, HubHeadInfo{HeadNum: c.head, LIBNum: c.lib})
			if c.wantCode == codes.OK {
				require.NoError(t, err)
				require.Equal(t, c.wantStart, got, "wantStart mismatch")
			} else {
				require.Error(t, err)
				code, _ := mustStatus(t, err)
				require.Equal(t, c.wantCode, code, "code mismatch")
			}
		})
	}
}
