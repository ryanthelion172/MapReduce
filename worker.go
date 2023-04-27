package main

import (
	"database/sql"
	"fmt"
	"hash/fnv"
	"log"
	"os"
	//"path/filepath"

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
	inputPath := tempdir + task.SourceHost
	//inputURL := filepath.Join(tempdir, makeURL(task.SourceHost, inputPath))
	//if err := download(inputURL, inputPath); err != nil {
	//	return err
	//}
	db, err := openDatabase(inputPath)
	if err != nil {
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
		paths[i] = mapOutputFile(task.N , i)
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
	rows, err := db.Query("select key, value from pairs")
	if err != nil {
		log.Printf("error in select query from database to split: %v", err)
		return err
	}
	for rows.Next() {
		messages := make(chan Pair)
		go func() {
			var key, value string
			if err := rows.Scan(&key, &value); err != nil {
				log.Printf("error scanning row value: %v", err)
			}
			// call client.map here
			if err := client.Map(key, value, messages); err != nil {
				log.Printf("error calling map: %v", err)
			}
		}()
		for {
			pair, isOkay := <-messages
			if isOkay != true {
				break
			}
			hash := fnv.New32()
			hash.Write([]byte(pair.Key))
			r := int(hash.Sum32() % uint32(task.R))
			insert := inserts[r]
			if _, err := insert.Exec(pair.Key, pair.Value); err != nil {
				log.Printf("db error inserting row to output database: %v ", err)
			}
		}
	}

	if err := rows.Err(); err != nil {
		log.Printf("db error iterating over inputs: %v", err)
		return err
	}

	return nil
}

func (task *ReduceTask) Process(tempdir string, client Interface) error {
	//create input database (merge)
	paths := make([]string, task.M)
	for i := 0; i < task.M; i++ {
		paths[i] = makeURL("localhost:8080", task.SourceHosts[i])
	}
	mergeDatabases(paths, reduceInputFile(task.N), reduceTempFile(task.N))
	for _, path := range paths {
		os.Remove(path)
	}
	//create output database
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
	out, err := createDatabase(reduceOutputFile(task.N))
	if err != nil {
		log.Printf("error creating databaseL %v", err)
		return err
	}
	outs = append(outs, out)
	insert, err := out.Prepare("insert into pairs (key, value) values (?, ?)")
	if err != nil {
		log.Printf("error preparing statement for output database: %v", err)
		return err
	}
	inserts = append(inserts, insert)
	//processs all the pairs

	return nil
}


