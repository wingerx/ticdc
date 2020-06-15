package backdate

import (
	"bufio"
	"io"
	"os"
	"strings"
	"testing"

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
	file, err := os.Open("cdc_crt_greater_than_rts_4.log")
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
		if strings.Contains(string(line), "Welcome") && strings.Contains(string(line), "04662ad2785540feae254666d4d354f1001a7924") {
			log.Info(string(line))
			find = true
		}
	}

	err = w.Flush()
	if err != nil {
		panic(err)
	}
}

func (s *backdateSuite) TestB(c *check.C) {
	file, err := os.Open("handle1.log")
	if err != nil {
		log.Fatal("", zap.Error(err))
	}
	defer file.Close()

	r := bufio.NewReader(file)
	var lastPrefixLine []byte
	i := 0
	for {
		if i > 100000 {
			break
		}
		i++
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
			println(string(line))
		}
	}
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
