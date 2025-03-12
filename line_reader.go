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
		line, err := r.Read()
		if err != nil {
			return "", nil
		}
		r.lineCount--

		r.buffer = &line
		return line, nil
	}

	return *r.buffer, nil
}

func (r *LineReader) Read() (string, error) {
	if r.buffer != nil {
		line := *r.buffer
		r.buffer = nil
		r.lineCount++
		return line, nil
	}

	r.lineCount++
	if r.scanner.Scan() {
		line := r.scanner.Text()
		ss := strings.SplitN(line, "#", 2)
		line = ss[0]
		return line, nil
	}

	if err := r.scanner.Err(); err == nil {
		return "", io.EOF
	} else {
		return "", err
	}
}

func (r *LineReader) Count() int {
	return r.lineCount
}
