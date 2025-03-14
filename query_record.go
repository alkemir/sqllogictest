package sqllogictest

import (
	"crypto/md5"
	"database/sql"
	"encoding/hex"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
)

var _ TestRecord = (*QueryRecord)(nil)

type QueryRecord struct {
	typeString string
	sortMode   string
	label      string
	query      string
	resultSize int
	resultHash string
	resultSet  []string
	LineReporter
}

func parseQuery(reader *LineReader) (*QueryRecord, error) {
	line, err := reader.Read()
	if err != nil {
		return nil, err
	}
	line = strings.TrimSpace(line)

	startLine := reader.Count()
	paramsSplit := strings.Split(line, " ")
	if len(paramsSplit) < 2 {
		return nil, fmt.Errorf("unexpected number of parameters for query on line %d: %d %q", reader.Count(), len(paramsSplit), line)
	}

	typeString := paramsSplit[1]
	sortMode := ""
	label := ""

	for i := 2; i < len(paramsSplit); i++ {
		switch paramsSplit[i] {
		case "nosort":
			if sortMode != "" {
				return nil, fmt.Errorf("illegal parameter for query (sort-mode) on line %d: %v", reader.Count(), paramsSplit)
			}
			sortMode = "nosort"

		case "rowsort":
			if sortMode != "" {
				return nil, fmt.Errorf("illegal parameter for query (sort-mode) on line %d: %v", reader.Count(), paramsSplit)
			}
			sortMode = "rowsort"

		case "valuesort":
			if sortMode != "" {
				return nil, fmt.Errorf("illegal parameter for query (sort-mode) on line %d: %v", reader.Count(), paramsSplit)
			}
			sortMode = "valuesort"

		default:
			if label != "" {
				return nil, fmt.Errorf("illegal parameter for query (label) on line %d: %v", reader.Count(), paramsSplit)
			}
			label = paramsSplit[i]
		}
	}
	if sortMode == "" {
		sortMode = "nosort"
	}

	query := ""
	for {
		line, err := reader.Read()
		if err != nil {
			return nil, fmt.Errorf("failed to parse query (query) on line %d: %w", reader.Count(), err)
		}

		if line == "" {
			return &QueryRecord{typeString: typeString, sortMode: sortMode, label: label, query: query, LineReporter: LineReporter{startLine: startLine, endLine: reader.Count()}}, nil
		}
		if line == "----" {
			break
		}

		query += line
	}

	resultSize := 0
	resultHash := ""
	line, err = reader.Read()
	if err != nil {
		if err == io.EOF {
			return &QueryRecord{typeString: typeString, sortMode: sortMode, label: label, query: query, resultSize: 0, LineReporter: LineReporter{startLine: startLine, endLine: reader.Count()}}, nil
		}
		return nil, fmt.Errorf("failed to parse query (results) on line %d: %w", reader.Count(), err)
	}
	_, err = fmt.Sscanf(line, "%d values hashing to %s", &resultSize, &resultHash)
	if err == nil || line == "" {
		return &QueryRecord{typeString: typeString, sortMode: sortMode, label: label, query: query, resultSize: resultSize, resultHash: resultHash, LineReporter: LineReporter{startLine: startLine, endLine: reader.Count()}}, nil
	}

	resultSet := make([]string, 0)
	resultSet = append(resultSet, line)
	for {
		line, err := reader.Read()
		if err != nil && err != io.EOF {
			return nil, fmt.Errorf("failed to parse query (result-set) on line %d: %w", reader.Count(), err)
		}

		if line == "" {
			return &QueryRecord{typeString: typeString, sortMode: sortMode, label: label, query: query, resultSet: resultSet, resultSize: len(resultSet), LineReporter: LineReporter{startLine: startLine, endLine: reader.Count()}}, nil
		}

		resultSet = append(resultSet, line)
	}
}

func (r *QueryRecord) Execute(ctx *TestContext) error {
	if ctx.shouldSkip() {
		return nil
	}

	rr, err := ctx.db().Query(r.query)
	if err != nil {
		return err
	}
	defer rr.Close()

	pointers := make([]any, 0)
	formats := make([]string, 0)
	for _, t := range r.typeString {
		pointers = append(pointers, &sql.NullString{})

		switch t {
		case 'T':
			formats = append(formats, "%s")
		case 'I':
			formats = append(formats, "%d")
		case 'R':
			formats = append(formats, "%.3f")
		default:
			return fmt.Errorf("could not parse type-string: %q", r.typeString)
		}
	}

	results := make([][]string, 0)
	for rr.Next() {
		if err := rr.Scan(pointers...); err != nil {
			return fmt.Errorf("could not scan row: %w", err)
		}

		rowResults := make([]string, 0)
		for i := 0; i < len(pointers); i++ {
			rowResults = append(rowResults, printValue(formats[i], pointers[i].(*sql.NullString)))
		}

		results = append(results, rowResults)
	}

	if r.sortMode == "rowsort" {
		sort.Slice(results, func(i int, j int) bool { return strings.Join(results[i], " ") < strings.Join(results[j], " ") })
	}

	allResults := make([]string, 0)
	for _, row := range results {
		allResults = append(allResults, row...)
	}
	if r.sortMode == "valuesort" {
		sort.Strings(allResults)
	}

	hasher := md5.New()
	for _, s := range allResults {
		hasher.Write([]byte(s))
		hasher.Write([]byte("\n"))
	}

	if r.resultHash != "" {
		if len(allResults) != r.resultSize {
			return fmt.Errorf("result set does not match. difference in size: %d vs %d\n%q", len(allResults), r.resultSize, r.query)
		}
		if hex.EncodeToString(hasher.Sum(nil)) != r.resultHash {
			return fmt.Errorf("result set does not match. difference in hash: %s vs %s\n%q", hex.EncodeToString(hasher.Sum(nil)), r.resultHash, r.query)
		}
	} else {
		if len(allResults) != len(r.resultSet) {
			return fmt.Errorf("result set does not match. difference in size: %d vs %d\nexpected: %v\nfound: %v\n%q", len(allResults), len(r.resultSet), r.resultSet, allResults, r.query)
		}

		for i := 0; i < len(results); i++ {
			if allResults[i] != r.resultSet[i] {
				return fmt.Errorf("result set does not match. difference found on index %d\nexpected: %v\nfound: %v\n%q", i, r.resultSet, allResults, r.query)
			}
		}
	}

	if err := rr.Err(); err != nil {
		if r.resultSize == 0 {
			return nil
		}
		return fmt.Errorf("error while iterating over result set: %w", err)
	}

	return nil
}

func printValue(f string, v *sql.NullString) string {
	if !v.Valid {
		return "NULL"
	}

	switch f {
	case "%s":
		if v.String == "" {
			return "(empty)"
		}
		return v.String
	case "%d":
		p, err := strconv.ParseFloat(v.String, 64)
		if err != nil {
			p = 0
		}
		return fmt.Sprintf(f, int(p))
	case "%.3f":
		p, err := strconv.ParseFloat(v.String, 64)
		if err != nil {
			p = 0
		}
		return fmt.Sprintf(f, p)
	default:
		panic("Invalid format string: " + f)
	}
}
