package main

import (
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"unicode"
)

func main() {
	m := 5
	ex, err := os.Executable()
	if err != nil {
		panic(err)
	}
	exPath := filepath.Dir(ex)
	source := exPath + "/austen.db"
	// tempdir := exPath + "/map"
	slice_of_source_files := make([]string, m)
	for i := 0; i < m; i++ {
		slice_of_source_files[i] = mapSourceFile(i)
	}
	err = splitDatabase(source, slice_of_source_files)
	if err != nil {
		log.Print(err)
	}

	urls := make([]string, m)
	for i := 0; i < m; i++ {
		urls[i] = makeURL("localhost:8080", slice_of_source_files[i])
	}

	go func() {
		address := ":8080"
		tempdir := exPath
		http.Handle("/data/", http.StripPrefix("/data", http.FileServer(http.Dir(tempdir))))
		if err := http.ListenAndServe(address, nil); err != nil {
			log.Printf("Error in HTTP server for %s: %v", address, err)
		}
	}()
	database, err := mergeDatabases(urls, "final_map.db", "temp.db")
	if err != nil {
		log.Print(err)
	}
	log.Print(database)
	for _, v := range slice_of_source_files {
		if err := os.Remove(v); err != nil {
			log.Printf("Error removing file: %v", err)
		}
	}
}

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

// databases := make([]string, 5)
// databases[0] = makeURL("localhost:8080", "austen-0.db")
// databases[1] = makeURL("localhost:8080", "austen-1.db")
// databases[2] = makeURL("localhost:8080", "austen-2.db")
// databases[3] = makeURL("localhost:8080", "austen-3.db")
// databases[4] = makeURL("localhost:8080", "austen-4.db")
// mergeDatabases(databases, "austen2.db", "temp.db")

// m := 9
// // r := 3
// ex, err := os.Executable()
// if err != nil {
// 	panic(err)
// }
// exPath := filepath.Dir(ex)
// source := exPath + "/austen.db"
// // tempdir := exPath + "/map"
// tempdir := filepath.Join(os.TempDir(), fmt.Sprintf("mapreduce.%d", os.Getpid()))
// defer os.RemoveAll(tempdir)
// log.Print(tempdir)
// slice_of_source_files := make([]string, m)
// for i := 0; i < m; i++ {
// 	slice_of_source_files[i] = "tmp/" + mapSourceFile(i)
// }

// err = splitDatabase(source, slice_of_source_files)
// if err != nil {
// 	log.Print(err)
// }
