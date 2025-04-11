package internal

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

const fsyncThreshold = 50

type WAL struct {
	file       *os.File
	writeCount int
}

func NewWAL(filepath string) (*WAL, error) {
	f, err := os.OpenFile(filepath, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}
	return &WAL{file: f, writeCount: 0}, nil
}


func (w *WAL) Append(op, key, value string, seq uint64) error {
	entry := fmt.Sprintf("%d|%s|%s|%s\n", seq, op, key, value)
	_, err := w.file.WriteString(entry)
	if err != nil {
		return err
	}

	w.writeCount++
	if w.writeCount >= fsyncThreshold {
		err = w.file.Sync()
		if err != nil {
			return err
		}
		w.writeCount = 0
	}
	return nil
}

func (w *WAL) Load(memtable *SkipList) error {
	w.file.Seek(0, 0)
	scanner := bufio.NewScanner(w.file)
	for scanner.Scan() {
		parts := strings.Split(scanner.Text(), "|")
		if len(parts) >= 4 {
			
			op := parts[1]
			key := parts[2]
			value := parts[3]

			switch op {
			case "PUT":
				memtable.Insert(key, value)
			case "DEL":
				memtable.Delete(key)
			}
		}
	}
	return scanner.Err()
}

func (w *WAL) Reset() error {
	w.file.Close()
	err := os.Remove(w.file.Name())
	if err != nil {
		return err
	}
	f, err := os.Create(w.file.Name())
	if err != nil {
		return err
	}
	w.file = f
	w.writeCount = 0
	return nil
}
