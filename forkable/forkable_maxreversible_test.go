// Copyright 2019 dfuse Platform Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package forkable

import (
	"testing"

	"github.com/streamingfast/bstream"
	"github.com/stretchr/testify/require"
)

// TestForkable_MaxReversibleBlocks reproduces the unbounded-reversible-buffer growth
// when chain LIB stalls (irreversibility stops advancing while head keeps moving).
// MoveLIB is the only eviction path, and ProcessBlock returns early (before MoveLIB)
// while LIB doesn't advance, so ForkDB.objects/links grow without bound -> OOM. This
// is the structural twin of the deleted gateCursor blowup. The cap makes the forkable
// fail fast (clean consumer restart) instead.
func TestForkable_MaxReversibleBlocks(t *testing.T) {
	prev := bstream.GetProtocolFirstStreamableBlock
	bstream.GetProtocolFirstStreamableBlock = 2
	defer func() { bstream.GetProtocolFirstStreamableBlock = prev }()

	// Blocks 1..3 advance LIB to 2 normally; from block 4 on, LIB stalls at 2 while
	// head keeps advancing -> the reversible buffer grows every block.
	stalled := func() []*bstream.Block {
		return []*bstream.Block{
			bTestBlockWithLIBNum("00000001a", "", 0),
			bTestBlockWithLIBNum("00000002a", "00000001a", 1),
			bTestBlockWithLIBNum("00000003a", "00000002a", 2),
			bTestBlockWithLIBNum("00000004a", "00000003a", 2),
			bTestBlockWithLIBNum("00000005a", "00000004a", 2),
			bTestBlockWithLIBNum("00000006a", "00000005a", 2),
			bTestBlockWithLIBNum("00000007a", "00000006a", 2),
			bTestBlockWithLIBNum("00000008a", "00000007a", 2),
		}
	}

	t.Run("control: no cap -> buffer grows unbounded, no error", func(t *testing.T) {
		fap := New(newTestForkableSink(nil, nil))
		for _, b := range stalled() {
			require.NoError(t, fap.ProcessBlock(b, b.ID()))
		}
		require.Greater(t, fap.forkDB.ReversibleBlockCount(), 3,
			"the LIB stall should have grown the reversible buffer past 3")
	})

	t.Run("with cap: fails fast once the buffer exceeds the cap", func(t *testing.T) {
		fap := New(newTestForkableSink(nil, nil), WithMaxReversibleBlocks(3))
		var err error
		for _, b := range stalled() {
			if err = fap.ProcessBlock(b, b.ID()); err != nil {
				break
			}
		}
		require.Error(t, err, "the forkable must fail fast when the reversible buffer exceeds the cap")
		require.Contains(t, err.Error(), "reversible buffer")
	})
}
