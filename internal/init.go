db, err := NewDB("wal.log") // You can pick a name/path for the WAL file
	if err != nil {
		fmt.Println("Failed to initialize DB:", err)
		return
	}

	// Insert a key-value pair
	db.Set("foo", "bar")

	// Create a snapshot
	snap := db.CreateSnapshot()

	// Query the snapshot
	val, ok := snap.Get("foo")
	if ok {
		fmt.Println("Snapshot Get:", val)
	} else {
		fmt.Println("Key not found in snapshot.")
	}

	// Change DB after snapshot creation
	db.Set("foo", "baz")

	// Query snapshot again (should still return old value)
	val, ok = snap.Get("foo")
	if ok {
		fmt.Println("Snapshot After DB Change:", val)
	} else {
		fmt.Println("Key not found in snapshot.")
	}
