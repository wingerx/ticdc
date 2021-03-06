// Copyright 2020 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package frontier

import (
	"math"
	"math/rand"
	"sort"

	"github.com/pingcap/check"
)

type tsHeapSuite struct{}

var _ = check.Suite(&tsHeapSuite{})

func (s *tsHeapSuite) insertIntoHeap(h *minTsHeap, ts uint64) *node {
	n := &node{ts: ts}
	h.insert(n)
	return n
}

func (s *tsHeapSuite) TestInsert(c *check.C) {
	var heap minTsHeap
	target := uint64(15000)

	for i := 0; i < 5000; i++ {
		s.insertIntoHeap(&heap, uint64(10001)+target+1)
	}
	s.insertIntoHeap(&heap, target)

	c.Assert(heap.getMin().ts, check.Equals, target)
}

func (s *tsHeapSuite) TestIncreaseTs(c *check.C) {
	rand.Seed(0xdeadbeaf)
	var heap minTsHeap
	nodes := make([]*node, 50000)
	for i := range nodes {
		nodes[i] = s.insertIntoHeap(&heap, uint64(rand.Intn(len(nodes)/2)))
	}
	sort.Slice(nodes, func(i, j int) bool { return nodes[i].ts < nodes[j].ts })

	for i := range nodes {
		min := heap.getMin().ts
		c.Assert(min, check.Equals, nodes[i].ts)
		heap.increaseTs(nodes[i], uint64(math.MaxUint64))
	}
}
