package sqllogictest

import (
	"fmt"
	"strings"
)

var _ TestRecord = (*OnlyIfRecord)(nil)

type OnlyIfRecord struct {
	dbName string
	LineReporter
}

func parseOnlyIf(reader *LineReader) (*OnlyIfRecord, error) {
	line, err := reader.Read()
	if err != nil {
		return nil, err
	}

	paramsSplit := strings.Split(line, " ")
	if len(paramsSplit) != 2 {
		return nil, fmt.Errorf("unexpected number of tokens for onlyif: %d %q", len(paramsSplit), line)
	}

	return &OnlyIfRecord{dbName: paramsSplit[1], LineReporter: LineReporter{startLine: reader.Count(), endLine: reader.Count()}}, nil
}

func (r *OnlyIfRecord) Execute(ctx *TestContext) error {
	ctx.onlyIf(r.dbName)
	return nil
}
