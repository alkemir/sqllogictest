package sqllogictest

import (
	"bufio"
	"database/sql"
	"io"
	"log"
)

type TestScript struct {
	records []TestRecord
}

type TestResult struct {
}

func ParseTestScript(r io.Reader) (*TestScript, error) {
	rr := make([]TestRecord, 0)

	lr := &LineReader{scanner: bufio.NewScanner(r)}
	for {
		testRecord, err := parseTestRecord(lr)
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}

		rr = append(rr, testRecord)
	}

	return &TestScript{records: rr}, nil
}

func (t *TestScript) Run(db *sql.DB, dbName string, logger *log.Logger) *TestResult {
	success := 0
	failure := 0
	ctx := &TestContext{dbHandle: db, dbName: dbName}
	for _, testRecord := range t.records {
		if err := testRecord.Execute(ctx); err != nil {
			logger.Printf("fail: %v\nwhile executing: %T\non lines: %d to %d\n", err, testRecord, testRecord.GetStartLine(), testRecord.GetEndLine())
			failure++
		} else {
			success++
		}
	}

	return &TestResult{}
}
