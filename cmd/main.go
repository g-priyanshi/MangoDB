package main

import (
	sstable "MangoDB/SSTable"
	"MangoDB/internal"
	"fmt"
	"log"
	"os"
)

// func main() {

// 	db.Put("name", "Alice")
// 	db.Put("city", "Wonderland")

// 	val, found := db.Get("name")
// 	if found {
// 		fmt.Println("GET name:", val)
// 	} else {
// 		fmt.Println("Key not found")
// 	}

// 	db.Delete("city")

// 	_, found = db.Get("city")
// 	if !found {
// 		fmt.Println("city successfully deleted")
// 	}

// 	// Force flush
// 	for i := 0; i < 5; i++ {
// 		db.Put(fmt.Sprintf("key%d", i), "value")
// 	}

// }
func RecoverDB() *internal.DB {
	db, _ := internal.NewDB("wal2.log")

	// Load from existing SSTables
	allEntries, _ := sstable.ReadAllTables("sstable")
	for _, e := range allEntries {
		fmt.Printf("Restoring: %s = %s\n", e.Key, e.Value)
		db.Put(e.Key, e.Value)
	}
	return db
}

func main() {
	// Clean old data
	os.Remove("wal.log")
	os.RemoveAll("sstable")
	//os.Mkdir("sstable", 0755),

	fmt.Println("=== First Run: Insert Entries and Simulate Crash ===")
	db, err := internal.NewDB("wal.log")
	if err != nil {
		log.Fatal(err)
	}
	for i := 1; i <= 1000; i++ {
		db.Put(fmt.Sprintf("key%02d", i), fmt.Sprintf("val%02d", i))
	}
	fmt.Println("Simulating crash...")

	// Simulate crash by ending this run (clears memory)
	// Recovery happens in new DB instance

	fmt.Println("\n=== Restart: Recover from SSTables + WAL ===")
	_ = RecoverDB()

	// // Print recovered data
	// fmt.Println("Recovered Memtable:")
	// for k, v := range db2.internal.GetAll() {
	// 	fmt.Printf("%s = %s\n", k, v)
	// }

	// // Add more entries
	// fmt.Println("\nInserting more after recovery...")
	// for i := 13; i <= 15; i++ {
	// 	db2.Put(fmt.Sprintf("key%02d", i), fmt.Sprintf("val%02d", i))
	// }

	// fmt.Println("\nFinal Memtable:")
	// for k, v := range db2.memtable.GetAll() {
	// 	fmt.Printf("%s = %s\n", k, v)
	// }










	
	//SNAPSHOT USAGE
	// db, err := internal.NewDB("wal.log")
	// if err != nil {
	// 	fmt.Println("Failed to initialize DB:", err)
	// 	return
	// }

	// // Step 2: Insert initial data
	// db.Put("foo", "bar")
	// db.Put("baz", "qux")

	// // Step 3: Create snapshot
	// snapshot := db.CreateSnapshot()

	// // Step 4: Save snapshot to file
	// err = snapshot.SaveToFile("snapshot.dat")
	// if err != nil {
	// 	fmt.Println("Error saving snapshot:", err)
	// 	return
	// }
	// fmt.Println("Snapshot saved successfully.")

	// // Step 5: Mutate the DB after snapshot
	// db.Put("foo", "new_value")
	// db.Put("baz", "updated")

	// // Step 6: Restore snapshot from file
	// restoredSnapshot, err := internal.RestoreSnapshot("snapshot.dat")
	// if err != nil {
	// 	fmt.Println("Error restoring snapshot:", err)
	// 	return
	// }
	// fmt.Println("Snapshot restored successfully.")

	// // Step 7: Validate values from restored snapshot
	// testKeys := []string{"foo", "baz"}
	// for _, key := range testKeys {
	// 	val, ok := restoredSnapshot.Get(key)
	// 	if ok {
	// 		fmt.Printf("Restored Snapshot Get %s: %s\n", key, val)
	// 	} else {
	// 		fmt.Printf("Key %s not found in restored snapshot.\n", key)
	// 	}
	// }
}
