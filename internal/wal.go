package internal

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

type WAL struct {
	file *os.File
}

func NewWAL(filepath string) (*WAL, error) {
	f, err := os.OpenFile(filepath, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}
	return &WAL{file: f}, nil
}

func (w *WAL) Append(op, key, value string) error {
	entry := fmt.Sprintf("%s|%s|%s\n", op, key, value)
	_, err := w.file.WriteString(entry)
	return err
}

func (w *WAL) Load(memtable *SkipList) error {
	w.file.Seek(0, 0)
	scanner := bufio.NewScanner(w.file)
	for scanner.Scan() {
		parts := strings.Split(scanner.Text(), "|")
		if len(parts) >= 2 {
			switch parts[0] {
			case "PUT":
				memtable.Insert(parts[1], parts[2])
			case "DEL":
				memtable.Delete(parts[1])
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
	return nil
}
