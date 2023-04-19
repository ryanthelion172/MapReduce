package main

import (
	"database/sql"
	"fmt"
	"log"
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
	db, err := openDatabase(inputPath)
	if err == nil {
		return err
	}
	defer db.Close()

	var outs []*sql.DB
	var inserts []*sql.Stmt
	defer func() {
		for i, insert := range inserts {
			if insert != nil {
				insert.Close()
			}
			inserts[i] = nil
		}
		for i, db := range outs {
			if db != nil {
				db.Close()
			}
			outs[i] = nil
		}
	}()
	paths := make([]string, task.R)
	for i := 0; i < task.R; i++ {
		paths[i] = "tmp/" + mapOutputFile(i, task.R)
	}
	for _, path := range paths {
		out, err := createDatabase(path)
		if err != nil {
			return err
		}
		outs = append(outs, out)
		insert, err := out.Prepare("insert into pairs (key, value) values (?, ?)")
		if err != nil {
			log.Printf("error preparing statement for output database: %v", err)
			return err
		}
		inserts = append(inserts, insert)
	}

	// process input pairs
	dbi := 0
	rows, err := db.Query("select key, value from pairs")
	if err != nil {
		log.Printf("error in select query from database to split: %v", err)
		return err
	}
	for rows.Next() {
		var key, value string
		if err := rows.Scan(&key, &value); err != nil {
			log.Printf("error scanning row value: %v", err)
			return err
		}

		// round-robin through the output databases
		insert := inserts[dbi]
		if _, err := insert.Exec(key, value); err != nil {
			log.Printf("db error inserting row to output database: %v", err)
			return err
		}
		dbi = (dbi + 1) % len(inserts)
	}
	if err := rows.Err(); err != nil {
		log.Printf("db error iterating over inputs: %v", err)
		return err
	}

	return nil
}
