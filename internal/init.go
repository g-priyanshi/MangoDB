package internal

import "fmt"

type DB struct {
	memtable *SkipList
	wal      *WAL
}

func NewDB(walPath string) (*DB, error) {
	wal, err := NewWAL(walPath)
	if err != nil {
		return nil, err
	}
	memtable := NewSkipList()

	db := &DB{
		memtable: memtable,
		wal:      wal,
	}

	// Load from WAL into memtable
	err = wal.Load(memtable)
	if err != nil {
		return nil, err
	}
	return db, nil
}

func (db *DB) Put(key, value string) error {
	err := db.wal.Append("PUT", key, value)
	if err != nil {
		return err
	}
	db.memtable.Insert(key, value)

	if db.memtable.IsFull() {
		err := db.Flush()
		if err != nil {
			return err
		}
	}
	return nil
}

func (db *DB) Get(key string) (string, bool) {
	return db.memtable.Search(key)
}

func (db *DB) Delete(key string) error {
	err := db.wal.Append("DEL", key, "")
	if err != nil {
		return err
	}
	db.memtable.Delete(key)
	return nil
}

func (db *DB) Flush() error {
	fmt.Println("Flushing memtable to SSTable...")
	// data := db.memtable.GetAll()

	// 📝 Assume this is your hook into existing SSTable implementation
	// SaveToSSTable(data)

	db.memtable.Reset()
	return db.wal.Reset()
}
