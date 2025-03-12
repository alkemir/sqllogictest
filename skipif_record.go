package sqllogictest

import (
	"fmt"
	"strings"
)

var _ TestRecord = (*SkipIfRecord)(nil)

type SkipIfRecord struct {
	dbName string
	LineReporter
}

func parseSkipIf(reader *LineReader) (*SkipIfRecord, error) {
	line, err := reader.Read()
	if err != nil {
		return nil, err
	}
	line = strings.TrimSpace(line)

	paramsSplit := strings.Split(line, " ")
	if len(paramsSplit) != 2 {
		return nil, fmt.Errorf("unexpected number of tokens for skipif on line %d: %d %q", reader.Count(), len(paramsSplit), line)
	}

	return &SkipIfRecord{dbName: paramsSplit[1], LineReporter: LineReporter{startLine: reader.Count(), endLine: reader.Count()}}, nil
}

func (r *SkipIfRecord) Execute(ctx *TestContext) error {
	ctx.skipIf(r.dbName)
	return nil
}
