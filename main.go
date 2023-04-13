package main

import (
	"log"
	"net/http"
	"os"
	"path/filepath"
)

func main() {
	ex, err := os.Executable()
	if err != nil {
		panic(err)
	}
	exPath := filepath.Dir(ex)
	slice_of_output_files := make([]string, 5)
	for i := 0; i < 5; i++ {
		slice_of_output_files[i] = mapOutputFile(i, 6)
	}
	source := exPath + "/austen.db"
	err = splitDatabase(source, slice_of_output_files)
	if err != nil {
		log.Print(err)
	}
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
