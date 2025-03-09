package sqllogictest

import (
	"database/sql"
	"slices"
)

type TestContext struct {
	dbName           string
	dbHandle         *sql.DB
	onlyIfClause     string
	skipIfClause     []string
	maxResultSetSize int
}

func (c *TestContext) shouldSkip() bool {
	skipIfClause := c.skipIfClause
	onlyIfClause := c.onlyIfClause
	c.skipIfClause = nil
	c.onlyIfClause = ""

	if slices.Contains(skipIfClause, c.dbName) {
		return true
	}
	if onlyIfClause != "" && onlyIfClause != c.dbName {
		return true
	}

	return false
}

func (c *TestContext) db() *sql.DB {
	return c.dbHandle
}

func (c *TestContext) skipIf(dbName string) {
	c.skipIfClause = append(c.skipIfClause, dbName)
}

func (c *TestContext) onlyIf(dbName string) {
	c.onlyIfClause = dbName
}

func (c *TestContext) hashThreshold(maxResultSetSize int) {
	c.maxResultSetSize = maxResultSetSize
}
