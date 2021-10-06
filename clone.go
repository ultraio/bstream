// Copyright 2020 dfuse Platform Inc.
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

package bstream

// CloneBlock copies the bstream.Block. It is useful when you want to
// use a local SubscriptionHub but want to modify the Block in
// sub-streams (with filtering for example). Make sure you call this
// before any call to `ToNative()`.
func CloneBlock(next Handler) HandlerFunc {
	return func(blk *Block, obj interface{}) (err error) {
		return next.ProcessBlock(blk.Clone(), obj)
	}
}
