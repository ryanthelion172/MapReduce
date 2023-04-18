package main

import (
	"database/sql"
	"fmt"
	"path/filepath"

	_ "github.com/mattn/go-sqlite3"
)

type MapTask struct {
	M, R       int    // total number of map and reduce tasks
	N          int    // map task number, 0-based
	SourceHost string // address of host with map input file
}

type ReduceTask struct {
	M, R        int      // total number of map and reduce tasks
	N           int      // reduce task number, 0-based
	SourceHosts []string // addresses of map workers
}

type Pair struct {
	Key   string
	Value string
}

type Interface interface {
	Map(key, value string, output chan<- Pair) error
	Reduce(key string, values <-chan string, output chan<- Pair) error
}

func mapSourceFile(m int) string       { return fmt.Sprintf("map_%d_source.db", m) }
func mapInputFile(m int) string        { return fmt.Sprintf("map_%d_input.db", m) }
func mapOutputFile(m, r int) string    { return fmt.Sprintf("map_%d_output_%d.db", m, r) }
func reduceInputFile(r int) string     { return fmt.Sprintf("reduce_%d_input.db", r) }
func reduceOutputFile(r int) string    { return fmt.Sprintf("reduce_%d_output.db", r) }
func reducePartialFile(r int) string   { return fmt.Sprintf("reduce_%d_partial.db", r) }
func reduceTempFile(r int) string      { return fmt.Sprintf("reduce_%d_temp.db", r) }
func makeURL(host, file string) string { return fmt.Sprintf("http://%s/data/%s", host, file) }

type output struct {
	db     *sql.DB
	insert *sql.Stmt
	path   string
}

func (task *MapTask) Process(tempdir string, client Interface) error {
	inputPath := tempdir + "/austen.db"
	inputURL := filepath.Join(tempdir, makeURL(task.SourceHost, inputPath))
	if err := download(inputURL, inputPath); err != nil {
		return err
	}
	inputfile, err := openDatabase(inputPath)
	if err == nil {
		return err
	}
	defer inputfile.Close()

	outs := make([]*sql.DB, task.R)
	for i := 0; i < task.R; i++ {
		db, err := createDatabase(tempdir + "tmp/" + mapOutputFile(i, task.R))
		if err != nil {
			return err
		}
		outs[i] = db
	}
	defer func() {
		for _, db := range outs {
			db.Close()
		}
	}()

	return nil
}
