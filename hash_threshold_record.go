package sqllogictest

import (
	"fmt"
	"strconv"
	"strings"
)

var _ TestRecord = (*HashThresholdRecord)(nil)

type HashThresholdRecord struct {
	maxResultSetSize int
	LineReporter
}

func parseHashThreshold(reader *LineReader) (*HashThresholdRecord, error) {
	line, err := reader.Read()
	if err != nil {
		return nil, err
	}

	paramsSplit := strings.Split(line, " ")
	if len(paramsSplit) != 2 {
		return nil, fmt.Errorf("unexpected number of tokens for hash-threshold: %d %q", len(paramsSplit), line)
	}
	maxResultSetSize, err := strconv.Atoi(paramsSplit[1])
	if err != nil {
		return nil, fmt.Errorf("failed to parse max result set size for hash-threshold: %q %v", paramsSplit[1], err)
	}

	return &HashThresholdRecord{maxResultSetSize: maxResultSetSize, LineReporter: LineReporter{startLine: reader.Count(), endLine: reader.Count()}}, nil
}

func (r *HashThresholdRecord) Execute(ctx *TestContext) error {
	ctx.hashThreshold(r.maxResultSetSize)
	return nil
}
