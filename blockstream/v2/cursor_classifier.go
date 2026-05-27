package blockstream

import (
	"math"

	"github.com/streamingfast/bstream/forkable"
	pbbstream "github.com/streamingfast/pbgo/dfuse/bstream/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// HubHeadInfo packages the head + LIB block numbers the classifier needs.
// Kept as a struct (rather than direct getters on Server) so the classifier
// stays a pure function and tests can supply synthetic head/LIB values.
//
// A zero-valued HeadNum or LIBNum is treated as "not available" by the
// classifier and disables the corresponding bounds check. This preserves
// pre-classifier behavior in deployments + tests where the Tracker has no
// getter registered for BlockStreamHeadTarget / BlockStreamLIBTarget.
type HubHeadInfo struct {
	HeadNum uint64
	LIBNum  uint64
}

// classifyCursor inspects request.StartCursor + request.StartBlockNum and
// returns either an effective start-block-num for FileSource iteration (as
// int64, preserving the relative-to-head negative-sentinel semantics that
// firehose.New + Tracker support natively), or a gRPC-coded error to send
// back to the client.
//
// Design ref: FIREHOSE_TARGET_ARCHITECTURE_v3.2_minimal.md §4.
//
// Bug-class context: replaces the per-call forkable.gateCursor /
// feedCursorStateRestorer path which accumulated ForkDB.objects unbounded
// when the cursor's block IDs were no longer canonical
// (FIREHOSE_FORKDB_GATE_BLOWUP_2026-05-18.md).
func classifyCursor(request *pbbstream.BlocksRequestV2, headInfo HubHeadInfo) (int64, error) {
	// Case A: no cursor → preserve existing StartBlockNum semantics, including
	// the negative-relative-to-head sentinel handled downstream by firehose.New
	// via Tracker.GetRelativeBlock.
	if request.StartCursor == "" {
		if headInfo.HeadNum > 0 && request.StartBlockNum >= 0 && uint64(request.StartBlockNum) > headInfo.HeadNum {
			return 0, status.Errorf(codes.InvalidArgument,
				"start block %d ahead of chain head %d",
				request.StartBlockNum, headInfo.HeadNum)
		}
		return request.StartBlockNum, nil
	}

	// Case B: cursor present → decode opaque payload
	cur, err := forkable.CursorFromOpaque(request.StartCursor)
	if err != nil {
		return 0, status.Errorf(codes.InvalidArgument, "invalid start cursor %q: %v", request.StartCursor, err)
	}

	// Case C: irreversible-step cursor → trivial path; covers 100% of dkafka
	// traffic after Phase 1 (DKAFKA_IRREVERSIBLE_FLIP_RUNBOOK.md). Irreversible
	// blocks cannot reorg, so cursor.Block at-or-below LIB is canonical without
	// any ID check.
	if cur.Step == forkable.StepIrreversible {
		// MaxUint64 guard: with HeadInfo unavailable (sentinel), the bounds
		// checks below short-circuit; an adversarial MaxUint64 cursor would
		// then overflow `Num()+1` to 0 and silently re-stream from genesis.
		// Reject at the boundary instead. Finding P2-A1.
		if cur.Block.Num() == math.MaxUint64 {
			return 0, status.Errorf(codes.InvalidArgument,
				"cursor block number overflow")
		}
		if headInfo.HeadNum > 0 && cur.Block.Num() > headInfo.HeadNum {
			return 0, status.Errorf(codes.InvalidArgument,
				"cursor block %d ahead of chain head %d",
				cur.Block.Num(), headInfo.HeadNum)
		}
		if headInfo.LIBNum > 0 && cur.Block.Num() > headInfo.LIBNum {
			return 0, status.Errorf(codes.OutOfRange,
				"irreversible cursor at block %d, but LIB is %d (cursor not yet irreversible)",
				cur.Block.Num(), headInfo.LIBNum)
		}
		return int64(cur.Block.Num() + 1), nil
	}

	// Case D: reversible-step cursor (NEW / UNDO) → not supported.
	// Per M6: add a branch when there's a real consumer.
	return 0, status.Errorf(codes.Unimplemented,
		"reversible-step cursors are not supported by this firehose; use irreversible-only or restart from start_block_num")
}
