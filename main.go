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

	urls := make([]string, m)
	for i := 0; i < m; i++ {
		urls[i] = makeURL("localhost:8080", slice_of_output_files[i])
	}
	database, err := mergeDatabases(urls, "final_map.db", "temp.db")
	if err != nil {
		log.Print(err)
	}
	log.Print(database)

	/*go func() {
		address := ":8080"
		tempdir := exPath + "/temp/"
		http.Handle("/data/", http.StripPrefix("/data", http.FileServer(http.Dir(tempdir))))
		if err := http.ListenAndServe(address, nil); err != nil {
			log.Printf("Error in HTTP server for %s: %v", address, err)
		}
	}()
	databases := make([]string, 5)
	databases[0] = makeURL("localhost:8080", "austen-0.db")
	databases[1] = makeURL("localhost:8080", "austen-1.db")
	databases[2] = makeURL("localhost:8080", "austen-2.db")
	databases[3] = makeURL("localhost:8080", "austen-3.db")
	databases[4] = makeURL("localhost:8080", "austen-4.db")
	mergeDatabases(databases, "austen2.db", "temp.db")*/

}
