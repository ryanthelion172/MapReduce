package main

import (
	"log"
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
}
