package main

import (
	"log"
	"os"
	"path/filepath"
)

func main() {
	m := 5
	r := 6
	ex, err := os.Executable()
	if err != nil {
		panic(err)
	}
	exPath := filepath.Dir(ex)
	source := exPath + "/austen.db"
	// tempdir := exPath + "/map"
	slice_of_output_files := make([]string, m)
	for i := 0; i < m; i++ {
		slice_of_output_files[i] = mapOutputFile(i, r)
	}
	err = splitDatabase(source, slice_of_output_files)
	if err != nil {
		log.Print(err)
	}
	// go func() {
	// 	http.Handle("/data/", http.StripPrefix("/data", http.FileServer(http.Dir(tempdir))))
	// 	if err := http.ListenAndServe("localhost:8080", nil); err != nil {
	// 		log.Printf("Error in HTTP server for %s: %v", "localhost:8080", err)
	// 	}
	// }()
	urls := make([]string, m)
	for i := 0; i < m; i++ {
		urls[i] = makeURL("localhost:8080", slice_of_output_files[i])
	}
	database, err := mergeDatabases(urls, "final_map.db", "temp.db")
	if err != nil {
		log.Print(err)
	}
	log.Print(database)
}
