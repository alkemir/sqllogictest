package sqllogictest

import (
	"bufio"
	"io"
	"strings"
)

type LineReader struct {
	scanner   *bufio.Scanner
	buffer    *string
	lineCount int
}

func NewLineReader(r io.Reader) *LineReader {
	return &LineReader{scanner: bufio.NewScanner(r)}
}

func (r *LineReader) Peek() (string, error) {
	if r.buffer == nil {
		line, err := r.Scan()
		if err != nil {
			return "", nil
		}
		r.lineCount--

		r.buffer = &line
		return line, nil
	}

	return *r.buffer, nil
}

func (r *LineReader) Scan() (string, error) {
	if r.buffer != nil {
		line := *r.buffer
		r.buffer = nil
		r.lineCount++
		return line, nil
	}

	r.lineCount++
	if r.scanner.Scan() {
		line := r.scanner.Text()
		return line, nil
	}

	if err := r.scanner.Err(); err == nil {
		return "", io.EOF
	} else {
		return "", err
	}
}

func (r *LineReader) Read() (string, error) {
	for {
		line, err := r.Scan()
		if err != nil {
			return "", err
		}

		if !strings.HasPrefix(line, "#") {
			return line, nil
		}
	}
}

func (r *LineReader) Count() int {
	return r.lineCount
}
