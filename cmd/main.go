package main

import (
	"fmt"
	"log"
	"internal/internal" // update this to your actual module path
)

func main() {
	db, err := internal.NewDB("wal.log")
	if err != nil {
		log.Fatal(err)
	}

	db.Put("name", "Alice")
	db.Put("city", "Wonderland")

	val, found := db.Get("name")
	if found {
		fmt.Println("GET name:", val)
	} else {
		fmt.Println("Key not found")
	}

	db.Delete("city")

	_, found = db.Get("city")
	if !found {
		fmt.Println("city successfully deleted")
	}

	// Force flush
	for i := 0; i < 1000; i++ {
		db.Put(fmt.Sprintf("key%d", i), "value")
	}
}
