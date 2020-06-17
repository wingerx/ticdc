package backdate

import (
	"bufio"
	"bytes"
	"encoding/base64"
	"io"
	"math"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/pingcap/ticdc/cdc/puller/frontier"
	"github.com/pingcap/ticdc/pkg/regionspan"

	"github.com/pingcap/log"
	"go.uber.org/zap"

	"github.com/pingcap/check"
)

type backdateSuite struct{}

var _ = check.Suite(&backdateSuite{})

func TestSuite(t *testing.T) {
	check.TestingT(t)
}

func (s *backdateSuite) ATestA(c *check.C) {
	file, err := os.Open("cdc_crt_greater_than_rts_5.log")
	if err != nil {
		log.Fatal("", zap.Error(err))
	}
	defer file.Close()

	file1, err := os.Create("handle1.log")
	if err != nil {
		log.Fatal("", zap.Error(err))
	}
	defer file.Close()
	r := bufio.NewReader(file)
	w := bufio.NewWriter(file1)
	defer w.Flush()
	var lastPrefixLine []byte

	find := false
	for {
		line, isPrefix, err := r.ReadLine()
		if err == io.EOF {
			break
		}
		if isPrefix {
			lastPrefixLine = append(lastPrefixLine, line...)
			continue
		}
		if len(lastPrefixLine) != 0 {
			line = append(lastPrefixLine, line...)
			lastPrefixLine = nil
		}
		if find {
			w.Write(line)
			w.WriteString("\n")
			continue
		}
		if strings.Contains(string(line), "Welcome") && strings.Contains(string(line), "0240f7fc9f933c36e70b2271e952efb692bcb669") {
			log.Info(string(line))
			find = true
		}
	}

	err = w.Flush()
	if err != nil {
		panic(err)
	}
}

func (s *backdateSuite) ATestB(c *check.C) {
	file, err := os.Open("handle1.log")
	if err != nil {
		log.Fatal("", zap.Error(err))
	}
	defer file.Close()

	file1, err := os.Create("handle2.log")
	if err != nil {
		log.Fatal("", zap.Error(err))
	}
	defer file.Close()
	w := bufio.NewWriter(file1)
	defer w.Flush()

	r := bufio.NewReader(file)
	var lastPrefixLine []byte
	for {
		line, isPrefix, err := r.ReadLine()
		if err == io.EOF {
			break
		}
		if isPrefix {
			lastPrefixLine = append(lastPrefixLine, line...)
			continue
		}
		if len(lastPrefixLine) != 0 {
			line = append(lastPrefixLine, line...)
			lastPrefixLine = nil
		}
		l := new(LogEvent)
		l.Parse(line)
		if l.Msg == "show puller span" && l.Params["table"] == "47" {
			_, err := w.Write(line)
			if err != nil {
				panic(err)
			}
			_, err = w.WriteString("\n")
			if err != nil {
				panic(err)
			}
		}
		if l.Msg == "Forward" && l.Params["tableID"] == "47" {
			_, err := w.Write(line)
			if err != nil {
				panic(err)
			}
			_, err = w.WriteString("\n")
			if err != nil {
				panic(err)
			}
		}
	}
	err = w.Flush()
	if err != nil {
		panic(err)
	}
}

func (s *backdateSuite) TestC(c *check.C) {
	file, err := os.Open("cdc_crt_greater_than_rts_6.log")
	if err != nil {
		log.Fatal("", zap.Error(err))
	}
	defer file.Close()

	r := bufio.NewReader(file)
	var lastPrefixLine []byte
	var f frontier.Frontier
	var lastResolvedTs uint64

	targetStartBase64 := "dIAAAAAAAAD/L19pgAAAAAD/AAABBAAAAAD/AIm0iwOAAAD/AAJVjUwAAAD8"
	targetEndBase64 := "dIAAAAAAAAD/L19pgAAAAAD/AAABBAAAAAD/AKZ3PwOAAAD/AAQoMOMAAAD8"
	targetStart, err := base64.StdEncoding.DecodeString(targetStartBase64)
	c.Assert(err, check.IsNil)
	targetEnd, err := base64.StdEncoding.DecodeString(targetEndBase64)
	c.Assert(err, check.IsNil)

	span := regionspan.Span{
		Start: targetStart,
		End:   targetEnd,
	}
	//totalSpan = span
	f = NewSimpleFrontier(span)

	//var totalSpan regionspan.Span
	for {
		line, isPrefix, err := r.ReadLine()
		if err == io.EOF {
			break
		}
		if isPrefix {
			lastPrefixLine = append(lastPrefixLine, line...)
			continue
		}
		if len(lastPrefixLine) != 0 {
			line = append(lastPrefixLine, line...)
			lastPrefixLine = nil
		}
		l := new(LogEvent)
		l.Parse(line)
		if l.Msg == "show puller span" && l.Params["table"] == "47" {
			if f != nil {
				panic("")
			}
			startBase64 := l.Params["start"]
			endBase64 := l.Params["end"]
			start, err := base64.StdEncoding.DecodeString(startBase64)
			c.Assert(err, check.IsNil)
			end, err := base64.StdEncoding.DecodeString(endBase64)
			c.Assert(err, check.IsNil)
			span := regionspan.Span{
				Start: start,
				End:   end,
			}
			//totalSpan = span
			f = NewSimpleFrontier(span)
			log.Info("new frontier", zap.Reflect("span", span))
		}
		if l.Msg == "Forward" && l.Params["tableID"] == "47" {
			//if f == nil {
			//	panic("")
			//}
			startBase64 := l.Params["start"]
			endBase64 := l.Params["end"]
			start, err := base64.StdEncoding.DecodeString(startBase64)
			c.Assert(err, check.IsNil)
			end, err := base64.StdEncoding.DecodeString(endBase64)
			c.Assert(err, check.IsNil)
			span := regionspan.Span{
				Start: start,
				End:   end,
			}
			spanRts, err := strconv.ParseUint(l.Params["spanResolvedTs"], 10, 64)
			c.Assert(err, check.IsNil)
			if spanRts < lastResolvedTs {
				panic("")
			}

			resolvedTs, err := strconv.ParseUint(l.Params["resolvedTs"], 10, 64)
			c.Assert(err, check.IsNil)
			lastResolvedTs = resolvedTs
			if bytes.Compare(targetStart, span.End) <= 0 && bytes.Compare(targetStart, span.Start) >= 0 {
				println(string(l.Source))
				f.Forward(span, spanRts)
				println(f.Frontier())
			} else if bytes.Compare(targetEnd, span.End) <= 0 && bytes.Compare(targetEnd, span.Start) >= 0 {
				println(string(l.Source))
				f.Forward(span, spanRts)
				println(f.Frontier())
			} else if bytes.Compare(targetEnd, span.End) >= 0 && bytes.Compare(targetStart, span.Start) <= 0 {
				println(string(l.Source))
				f.Forward(span, spanRts)
				println(f.Frontier())
			}
			//c.Assert(resolvedTs, check.Equals, f.Frontier())
			//log.Info("", zap.Uint64("", f.Frontier()))
			//	println(string(l.Source))
			//f.Forward(span, spanRts)
			//	println(f.Frontier())
		}
		if l.Msg == "start new request" {
			//reqStr := l.Params["request"]
			//reqStr = reqStr[1 : len(reqStr)-1]
			//reqStr = strings.ReplaceAll(reqStr, `\"`, `"`)
			//req := request{}
			//err := json.Unmarshal([]byte(reqStr), &req)
			//c.Assert(err, check.IsNil)
			//if regionspan.KeyInSpan(req.StartKey, totalSpan) || regionspan.KeyInSpan(req.EndKey, totalSpan) {
			//	//println(req.CheckpointTs)
			//	//c.Assert(req.CheckpointTs, check.GreaterEqual, lastResolvedTs)
			//	if req.CheckpointTs <= lastResolvedTs {
			//		println(lastResolvedTs)
			//		println(string(l.Source))
			//
			//	}
			//}

		}
	}
}

type request struct {
	CheckpointTs uint64 `json:"checkpoint_ts"`
	StartKey     []byte `json:"start_key"`
	EndKey       []byte `json:"end_key"`
}

type LogEvent struct {
	Time   string
	Level  string
	Code   string
	Msg    string
	Params map[string]string
	Source []byte
}

func (e *LogEvent) Parse(s []byte) {
	e.Source = s
	var split [][]byte
	lastLeft := -1
	redundantLeft := 0
	for i := 0; i < len(s); i++ {
		switch s[i] {
		case '[':
			if lastLeft == -1 {
				lastLeft = i
			} else {
				redundantLeft++
			}
		case ']':
			if redundantLeft == 0 {
				split = append(split, s[lastLeft+1:i])
				lastLeft = -1
			} else {
				redundantLeft--
			}
		}
	}
	e.Time = string(split[0])
	e.Level = string(split[1])
	e.Code = string(split[2])
	e.Msg = strings.TrimFunc(string(split[3]), func(r rune) bool {
		return r == '"'
	})
	if e.Params == nil {
		e.Params = make(map[string]string)
	}
	for i := 4; i < len(split); i++ {
		sps := strings.SplitN(string(split[i]), "=", 2)
		e.Params[sps[0]] = sps[1]
	}
}

func (s *backdateSuite) ATestD(c *check.C) {
	f := NewSimpleFrontier(regionspan.Span{
		Start: []byte{100},
		End:   []byte{200},
	})
	c.Assert(f.points, check.DeepEquals, []*keyWithTs{
		{key: []byte{100}, ts: 0},
		{key: []byte{200}, ts: math.MaxUint64},
	})
	f.Forward(regionspan.Span{
		Start: []byte{120},
		End:   []byte{180},
	}, 10)
	c.Assert(f.points, check.DeepEquals, []*keyWithTs{
		{key: []byte{100}, ts: 0},
		{key: []byte{120}, ts: 10},
		{key: []byte{180}, ts: 0},
		{key: []byte{200}, ts: math.MaxUint64},
	})
	f.Forward(regionspan.Span{
		Start: []byte{110},
		End:   []byte{150},
	}, 20)
	c.Assert(f.points, check.DeepEquals, []*keyWithTs{
		{key: []byte{100}, ts: 0},
		{key: []byte{110}, ts: 20},
		{key: []byte{150}, ts: 10},
		{key: []byte{180}, ts: 0},
		{key: []byte{200}, ts: math.MaxUint64},
	})
	f.Forward(regionspan.Span{
		Start: []byte{90},
		End:   []byte{160},
	}, 30)
	c.Assert(f.points, check.DeepEquals, []*keyWithTs{
		{key: []byte{100}, ts: 30},
		{key: []byte{160}, ts: 10},
		{key: []byte{180}, ts: 0},
		{key: []byte{200}, ts: math.MaxUint64},
	})
	f.Forward(regionspan.Span{
		Start: []byte{140},
		End:   []byte{240},
	}, 40)
	c.Assert(f.points, check.DeepEquals, []*keyWithTs{
		{key: []byte{100}, ts: 30},
		{key: []byte{140}, ts: 40},
		{key: []byte{200}, ts: math.MaxUint64},
	})
	f.Forward(regionspan.Span{
		Start: []byte{140},
		End:   []byte{200},
	}, 50)
	c.Assert(f.points, check.DeepEquals, []*keyWithTs{
		{key: []byte{100}, ts: 30},
		{key: []byte{140}, ts: 50},
		{key: []byte{200}, ts: math.MaxUint64},
	})
	f.Forward(regionspan.Span{
		Start: []byte{0},
		End:   []byte{255},
	}, 60)
	c.Assert(f.points, check.DeepEquals, []*keyWithTs{
		{key: []byte{100}, ts: 60},
		{key: []byte{200}, ts: math.MaxUint64},
	})
	f.Forward(regionspan.Span{
		Start: []byte{253},
		End:   []byte{255},
	}, 70)
	c.Assert(f.points, check.DeepEquals, []*keyWithTs{
		{key: []byte{100}, ts: 60},
		{key: []byte{200}, ts: math.MaxUint64},
	})
}
