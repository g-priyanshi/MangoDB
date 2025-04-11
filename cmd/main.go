package main

import (
	"MangoDB/internal"
	"fmt"
)

func main() {

	db, err := internal.NewDB("wal.log")
	if err != nil {
		fmt.Println("Failed to initialize DB:", err)
		return
	}
	for i := 0; i < 55; i++ {
		key := fmt.Sprintf("key%03d", i)
		value := fmt.Sprintf("val%03d", i)
		db.Put(key, value)
	}
	snapshot := db.CreateSnapshot()
	_ = snapshot.SaveToFile("snapshot.dat")
	restoredSnapshot, err := internal.RestoreSnapshot("snapshot.dat")
	if err != nil {
		fmt.Println("Failed to restore snapshot:", err)
		return
	}

	fmt.Println("Restored Sequence:", restoredSnapshot.Sequence)

	// Check a few keys in the memtable
	for i := 50; i < 55; i++ {
		key := fmt.Sprintf("key%03d", i)
		value, ok := restoredSnapshot.Memtable.Get(key)
		if ok {
			fmt.Printf("Memtable[%s] = %s\n", key, value)
		} else {
			fmt.Printf("Memtable[%s] not found\n", key)
		}

	}

	// Print entries from SSTables
	for i, sst := range restoredSnapshot.SSTables {
		fmt.Printf("SSTable %d:\n", i)
		for _, entry := range sst {
			fmt.Printf("  %s => %s\n", entry.Key, entry.Value)
		}
	}

}
