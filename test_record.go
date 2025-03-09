package sqllogictest

import (
	"fmt"
	"strings"
)

type TestRecord interface {
	Execute(ctx *TestContext) error
	GetStartLine() int
	GetEndLine() int
}

func parseTestRecord(reader *LineReader) (TestRecord, error) {
	var line string
	var err error

	// Skip empty lines
	for {
		line, err = reader.Peek()
		if err != nil {
			return nil, err
		}

		if line != "" {
			break
		}

		_, err = reader.Peek()
		if err != nil {
			return nil, err
		}
	}

	switch {
	case strings.HasPrefix(line, "statement"):
		return parseStatement(reader)
	case strings.HasPrefix(line, "query"):
		return parseQuery(reader)
	case strings.HasPrefix(line, "halt"):
		return parseHalt(reader)
	case strings.HasPrefix(line, "hash-threshold"):
		return parseHashThreshold(reader)
	case strings.HasPrefix(line, "skipif"):
		return parseSkipIf(reader)
	case strings.HasPrefix(line, "onlyif"):
		return parseOnlyIf(reader)
	default:
		return nil, fmt.Errorf("unknown TestRecord sentence: %q", line)
	}
}
