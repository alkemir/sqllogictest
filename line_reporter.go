package sqllogictest

type LineReporter struct {
	startLine int
	endLine   int
}

func (r *LineReporter) GetStartLine() int {
	return r.startLine
}

func (r *LineReporter) GetEndLine() int {
	return r.endLine
}
