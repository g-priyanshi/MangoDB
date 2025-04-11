package internal

import (
	sstable "MangoDB/SSTable"
	"fmt"
)

type DB struct {
	memtable *SkipList
	wal      *WAL
	seq      uint64
	sstables [][]sstable.Entry
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
		seq:      0, // ✅ Start sequence from 0; can be loaded from snapshot later
	}

	// Load from WAL into memtable
	err = wal.Load(memtable)
	if err != nil {
		return nil, err
	}
	return db, nil
}

func (db *DB) Put(key, value string) error {
	db.seq++ // ✅ Increment sequence number

	err := db.wal.Append("PUT", key, value, db.seq) // ✅ Pass seq to WAL
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
	db.seq++ // ✅ Increment sequence number

	err := db.wal.Append("DEL", key, "", db.seq) // ✅ Pass seq to WAL
	if err != nil {
		return err
	}
	db.memtable.Delete(key)
	return nil
}

func (db *DB) Flush() error {
	fmt.Println("Flushing memtable to SSTable...")
	dataMap := db.memtable.GetAll()
	entries := make([]sstable.Entry, 0, len(dataMap))
	for k, v := range dataMap {
		entries = append(entries, sstable.Entry{
			Key:   k,
			Value: v,
			// SequenceNumber will be set by WriteSSTables
		})
	}

	// Sort entries inside WriteSSTables
	newSeq, err := sstable.WriteSSTables("sstable", entries, 50, 50, db.seq)
	if err != nil {
		return err
	}

	db.seq = newSeq
	// 📝 Hook into your SSTable implementation
	// SaveToSSTable(data)

	db.memtable.Reset()
	return db.wal.Reset()
}

func (db *DB) CreateSnapshot() *Snapshot {
	return &Snapshot{
		Memtable: db.memtable.Clone(), // implement Clone() in SkipList
		SSTables: db.sstables,         // reuse existing SSTable entries
		Sequence: db.seq,
		Released: false,
	}
}
