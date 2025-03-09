package sqllogictest

import (
	"fmt"
)

var _ TestRecord = (*HaltRecord)(nil)

type HaltRecord struct {
	LineReporter
}

func parseHalt(reader *LineReader) (*HaltRecord, error) {
	_, err := reader.Read()
	if err != nil {
		return nil, err
	}

	return &HaltRecord{LineReporter: LineReporter{startLine: reader.Count(), endLine: reader.Count()}}, nil
}

func (r *HaltRecord) Execute(ctx *TestContext) error {
	return fmt.Errorf("halt statement. context: %v", ctx)
}
