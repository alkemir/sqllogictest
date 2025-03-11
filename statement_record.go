package sqllogictest

import (
	"fmt"
	"io"
	"strings"
)

var _ TestRecord = (*StatementRecord)(nil)

type StatementRecord struct {
	statement   string
	shouldError bool
	LineReporter
}

func parseStatement(reader *LineReader) (*StatementRecord, error) {
	line, err := reader.Read()
	if err != nil {
		return nil, err
	}

	startLine := reader.Count()
	paramsSplit := strings.Split(line, " ")
	if len(paramsSplit) != 2 {
		return nil, fmt.Errorf("unexpected number of tokens for statement: %d %q", len(paramsSplit), line)
	}

	var shouldError bool
	switch paramsSplit[1] {
	case "ok":
		shouldError = false
	case "error":
		shouldError = true
	default:
		return nil, fmt.Errorf("unexpected result for statement: %q", paramsSplit[1])
	}

	statement := ""
	for {
		line, err := reader.Read()
		if err != nil && err != io.EOF {
			return nil, fmt.Errorf("could not read sentence for statement: %v", err)
		}

		if line == "" {
			break
		}

		statement += line
	}

	return &StatementRecord{shouldError: shouldError, statement: statement, LineReporter: LineReporter{startLine: startLine, endLine: reader.Count()}}, nil
}

func (r *StatementRecord) Execute(ctx *TestContext) error {
	if ctx.shouldSkip() {
		return nil
	}

	_, err := ctx.db().Exec(r.statement)
	if (err != nil) != r.shouldError {
		return fmt.Errorf("unexpected result from statement: %q shouldError: %v error: %v", r.statement, r.shouldError, err)
	}
	return nil
}
