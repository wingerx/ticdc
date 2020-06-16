package backdate

import (
	"bytes"
	"math"

	"github.com/pingcap/ticdc/pkg/regionspan"
)

type SimpleFrontier struct {
	points []*keyWithTs
}

type keyWithTs struct {
	key []byte
	ts  uint64
}

func NewSimpleFrontier(span regionspan.Span) *SimpleFrontier {
	return &SimpleFrontier{
		points: []*keyWithTs{{
			key: span.Start,
			ts:  0,
		}, {
			key: span.End,
			ts:  math.MaxUint64,
		}},
	}
}

func (s *SimpleFrontier) Forward(span regionspan.Span, ts uint64) {
	startIndex := -1
	endIndex := -1
	for i := 0; i < len(s.points); i++ {
		cmp := bytes.Compare(s.points[i].key, span.Start)
		if cmp >= 0 {
			startIndex = i
			break
		}
	}
	for i := len(s.points) - 1; i >= 0; i-- {
		cmp := bytes.Compare(s.points[i].key, span.End)
		if cmp <= 0 {
			endIndex = i
			break
		}
	}
	if startIndex < 0 || endIndex < 0 {
		return
	}

	// 特殊处理边界
	if startIndex == 0 {
		if bytes.Compare(s.points[0].key, span.Start) < 0 {
			panic("")
		}
		span.Start = s.points[0].key
	}
	if endIndex == len(s.points)-1 {
		if bytes.Compare(s.points[len(s.points)-1].key, span.End) > 0 {
			panic("")
		}
		span.End = s.points[len(s.points)-1].key
	}

	newPoints := make([]*keyWithTs, 0, len(s.points)+2)
	for i := 0; i < startIndex; i++ {
		newPoints = append(newPoints, s.points[i])
	}
	endTs := s.points[endIndex].ts
	newPoints = append(newPoints,
		&keyWithTs{
			key: span.Start,
			ts:  ts,
		},
		&keyWithTs{
			key: span.End,
			ts:  endTs,
		})
	for i := endIndex + 1; i < len(s.points); i++ {
		newPoints = append(newPoints, s.points[i])
	}
	s.points = newPoints
}

func (s SimpleFrontier) Frontier() uint64 {
	minTs := uint64(math.MaxUint64)
	for _, st := range s.points {
		if minTs > st.ts {
			minTs = st.ts
		}
	}
	return minTs
}

func (s SimpleFrontier) String() string {
	panic("implement me")
}
