# What is this?
A harness to run [sqllogictests](https://www.sqlite.org/sqllogictest/doc/trunk/about.wiki) against any sql compliant engine.

# How to use it?

Get a copy of the test files (not included because of size) and put them in the ./test folder. Then, run something like the following.

```golang

package main

import (
	"database/sql"
	"fmt"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/alkemir/sqllogictest"
	_ "github.com/mattn/go-sqlite3"
)

func main() {
	fmt.Println("Running tests in test/")
	filepath.Walk("./test", runTest)
	os.Remove("./test.db")
}

func runTest(fpath string, info fs.FileInfo, err error) error {
	if err != nil {
		return err
	}
	if info.IsDir() {
		return nil
	}
	if !strings.HasSuffix(fpath, ".test") {
		return nil
	}

	os.Remove("./test.db")
	db, err := sql.Open("sqlite3", "./test.db")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	fp, err := os.Open(fpath)
	if err != nil {
		log.Fatal(err)
	}
	testScript, err := sqllogictest.ParseTestScript(fp)
	if err != nil {
		log.Fatalf("%s: %v", fpath, err)
	}

	startTime := time.Now()
	res := testScript.Run(db, "sqlite", true, log.Default())
	runTime := time.Since(startTime)

	successRate := 0
	if res.Success()+res.Failure() != 0 {
		successRate = (100 * res.Success()) / (res.Success() + res.Failure())
	}
	fmt.Printf("%s: %d%% (%v)\n", fpath, successRate, runTime)
	return nil
}
```