package main

import (
	"fmt"
	"log"
	"strconv"
	"strings"
	"unicode"

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

type Client struct{}

func (c Client) Map(key, value string, output chan<- Pair) error {
    defer close(output)
    lst := strings.Fields(value)
    for _, elt := range lst {
        word := strings.Map(func(r rune) rune {
            if unicode.IsLetter(r) || unicode.IsDigit(r) {
                    return unicode.ToLower(r)
            }
            return -1
        }, elt)
        if len(word) > 0 {
            output <- Pair{Key: word, Value: "1"}
        }
    }
    return nil
}

func (c Client) Reduce(key string, values <-chan string, output chan<- Pair) error {
    defer close(output)
    count := 0
    for v := range values {
        i, err := strconv.Atoi(v)
        if err != nil {
            return err
        }
        count += i
    }
    p := Pair{Key: key, Value: strconv.Itoa(count)}
    output <- p
    return nil
}

func (task *MapTask) Process(tempdir string, client Interface) error {
	slice_of_output_files := make([]string, task.M)

	source_file := mapSourceFile(task.M)
	input_file := mapInputFile(task.M)
	for i := 0; i < task.M; i++ {
		slice_of_output_files[i] = mapOutputFile(i, task.R)
	}
	err := download(input_file, tempdir)
	if err != nil {
		log.Printf("error downloading the file with error:%d", err)
		return err
	}
	splitDatabase(source_file, slice_of_output_files)

	// output_files, err := splitDatabase(task.SourceHost, outputDir, outputPattern string, m int)
	return nil
}
