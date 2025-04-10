package main

import (
	"fmt"
	"mykvstore/kvstore"
)

func main() {
	db := kvstore.NewKVStore()

	db.Put("apple", "fruit")
	db.Put("zebra", "animal")
	db.Put("ball", "toy")
	db.Put("pen", "stationery")
	db.Put("google", "company") // triggers flush
	db.Put("mango", "fruit")

	fmt.Println("Get apple:", safeGet(db, "apple"))
	fmt.Println("Get zebra:", safeGet(db, "zebra"))

	db.Delete("zebra")
	fmt.Println("Deleted zebra.")
	fmt.Println("Get zebra:", safeGet(db, "zebra"))
}

func safeGet(db *kvstore.KVStore, key string) string {
	val, ok := db.Get(key)
	if ok {
		return val
	}
	return "<not found>"
}
