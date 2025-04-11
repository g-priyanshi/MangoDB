package sstable

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io/ioutil"
	"os"
	"regexp"
	"sort"
)

type Entry struct {
	Key            string
	Value          string
	SequenceNumber uint64
}

type DataBlock struct {
	Entries []Entry
}

func (db *DataBlock) Encode() []byte {
	buf := new(bytes.Buffer)
	for _, entry := range db.Entries {
		binary.Write(buf, binary.LittleEndian, int32(len(entry.Key)))
		buf.Write([]byte(entry.Key))
		binary.Write(buf, binary.LittleEndian, int32(len(entry.Value)))
		buf.Write([]byte(entry.Value))
		binary.Write(buf, binary.LittleEndian, entry.SequenceNumber)
	}
	return buf.Bytes()
}

type IndexEntry struct {
	Key    string
	Offset int64
}

type FilterBlock struct {
	Filter map[string]bool
}

func (fb *FilterBlock) Add(key string) {
	fb.Filter[key] = true
}

func (fb *FilterBlock) MightContain(key string) bool {
	return fb.Filter[key]
}

func getSSTableFilename(index int) string {
	return "sstable_" + fmt.Sprintf("%d", index) + ".sst"
}

func writeSingleSSTable(filename string, entries []Entry, blockSize int) error {
	tempFilename := filename + ".tmp"
	file, err := os.Create(tempFilename)
	if err != nil {
		return err
	}
	defer file.Close()

	index := []IndexEntry{}
	filter := FilterBlock{Filter: make(map[string]bool)}

	offset := int64(0)
	block := DataBlock{}
	for i, entry := range entries {
		block.Entries = append(block.Entries, entry)
		filter.Add(entry.Key)
		if len(block.Entries) == blockSize || i == len(entries)-1 {
			blockData := block.Encode()
			checksum := crc32.ChecksumIEEE(blockData)
			_, err := file.Write(blockData)
			if err != nil {
				return err
			}
			err = binary.Write(file, binary.LittleEndian, checksum)
			if err != nil {
				return err
			}
			index = append(index, IndexEntry{Key: block.Entries[0].Key, Offset: offset})
			offset += int64(len(blockData) + 4)
			block.Entries = nil
		}
	}

	indexOffset := offset
	for _, ie := range index {
		err := binary.Write(file, binary.LittleEndian, int32(len(ie.Key)))
		if err != nil {
			return err
		}
		_, err = file.Write([]byte(ie.Key))
		if err != nil {
			return err
		}
		err = binary.Write(file, binary.LittleEndian, ie.Offset)
		if err != nil {
			return err
		}
	}

	footer := make([]byte, 8)
	binary.LittleEndian.PutUint64(footer, uint64(indexOffset))
	_, err = file.Write(footer)
	if err != nil {
		return err
	}

	file.Close()

	err = os.Rename(tempFilename, filename)
	if err != nil {
		return err
	}

	return nil
}

func getNextSSTableIndex(baseFilename string) int {
	files, err := ioutil.ReadDir(".")
	if err != nil {
		return 0
	}
	pattern := fmt.Sprintf(`^%s_(\d+)\.sst$`, baseFilename)
	re := regexp.MustCompile(pattern)
	maxIndex := -1
	for _, file := range files {
		match := re.FindStringSubmatch(file.Name())
		if len(match) > 1 {
			var idx int
			fmt.Sscanf(match[1], "%d", &idx)
			if idx > maxIndex {
				maxIndex = idx
			}
		}
	}
	return maxIndex + 1
}
func WriteSSTables(baseFilename string, entries []Entry, blockSize int, entriesPerSSTable int, startSeq uint64) ([][]Entry, uint64, error) {
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].Key < entries[j].Key
	})

	sstableIndex := getNextSSTableIndex(baseFilename)
	currSeq := startSeq

	var result [][]Entry

	for i := 0; i < len(entries); i += entriesPerSSTable {
		end := i + entriesPerSSTable
		if end > len(entries) {
			end = len(entries)
		}
		subEntries := entries[i:end]

		for j := range subEntries {
			subEntries[j].SequenceNumber = currSeq
			currSeq++
		}

		filename := fmt.Sprintf("%s_%d.sst", baseFilename, sstableIndex)
		err := writeSingleSSTable(filename, subEntries, blockSize)
		if err != nil {
			return nil, currSeq, err
		}

		result = append(result, subEntries)
		sstableIndex++
	}

	return result, currSeq, nil
}

func decodeDataBlock(data []byte) ([]Entry, error) {
	var entries []Entry
	buf := bytes.NewReader(data)
	for buf.Len() > 0 {
		var keyLen int32
		if err := binary.Read(buf, binary.LittleEndian, &keyLen); err != nil {
			return nil, err
		}
		keyBytes := make([]byte, keyLen)
		if _, err := buf.Read(keyBytes); err != nil {
			return nil, err
		}

		var valLen int32
		if err := binary.Read(buf, binary.LittleEndian, &valLen); err != nil {
			return nil, err
		}
		valBytes := make([]byte, valLen)
		if _, err := buf.Read(valBytes); err != nil {
			return nil, err
		}

		var seqNum uint64
		if err := binary.Read(buf, binary.LittleEndian, &seqNum); err != nil {
			return nil, err
		}

		entries = append(entries, Entry{
			Key:            string(keyBytes),
			Value:          string(valBytes),
			SequenceNumber: seqNum,
		})
	}
	return entries, nil
}

func ReadAllTables(baseFilename string) ([]Entry, error) {
	files, err := ioutil.ReadDir(".")
	if err != nil {
		return nil, err
	}

	pattern := fmt.Sprintf(`^%s_(\d+)\.sst$`, baseFilename)
	re := regexp.MustCompile(pattern)

	var allEntries []Entry

	for _, file := range files {
		if !re.MatchString(file.Name()) {
			continue
		}

		data, err := os.ReadFile(file.Name())
		if err != nil {
			return nil, err
		}

		if len(data) < 8 {
			continue
		}
		indexOffset := binary.LittleEndian.Uint64(data[len(data)-8:])
		indexData := data[indexOffset : len(data)-8]

		var index []IndexEntry
		buf := bytes.NewReader(indexData)
		for buf.Len() > 0 {
			var keyLen int32
			if err := binary.Read(buf, binary.LittleEndian, &keyLen); err != nil {
				break
			}
			key := make([]byte, keyLen)
			if _, err := buf.Read(key); err != nil {
				break
			}
			var offset int64
			if err := binary.Read(buf, binary.LittleEndian, &offset); err != nil {
				break
			}
			index = append(index, IndexEntry{Key: string(key), Offset: offset})
		}

		for i, idx := range index {
			start := idx.Offset
			var end int64
			if i+1 < len(index) {
				end = index[i+1].Offset
			} else {
				end = int64(indexOffset)
			}
			blockData := data[start : end-4] 
			entries, err := decodeDataBlock(blockData)
			if err != nil {
				return nil, err
			}
			allEntries = append(allEntries, entries...)
		}
	}

	return allEntries, nil
}

func ReadValueByKey(baseFilename string, key string) (Entry, bool, error) {
	files, err := ioutil.ReadDir(".")
	if err != nil {
		return Entry{}, false, err
	}

	pattern := fmt.Sprintf(`^%s_(\d+)\.sst$`, baseFilename)
	re := regexp.MustCompile(pattern)

	for _, file := range files {
		if !re.MatchString(file.Name()) {
			continue
		}

		data, err := os.ReadFile(file.Name())
		if err != nil {
			return Entry{}, false, err
		}

		if len(data) < 8 {
			continue
		}
		indexOffset := binary.LittleEndian.Uint64(data[len(data)-8:])
		indexData := data[indexOffset : len(data)-8]

		var index []IndexEntry
		buf := bytes.NewReader(indexData)
		for buf.Len() > 0 {
			var keyLen int32
			if err := binary.Read(buf, binary.LittleEndian, &keyLen); err != nil {
				break
			}
			keyBytes := make([]byte, keyLen)
			if _, err := buf.Read(keyBytes); err != nil {
				break
			}
			var offset int64
			if err := binary.Read(buf, binary.LittleEndian, &offset); err != nil {
				break
			}
			index = append(index, IndexEntry{Key: string(keyBytes), Offset: offset})
		}


		for i, idx := range index {
			start := idx.Offset
			var end int64
			if i+1 < len(index) {
				end = index[i+1].Offset
			} else {
				end = int64(indexOffset)
			}
			blockData := data[start : end-4]
			entries, err := decodeDataBlock(blockData)
			if err != nil {
				return Entry{}, false, err
			}
			for _, entry := range entries {
				if entry.Key == key {
					return entry, true, nil
				}
			}

		}
	}
	return Entry{}, false, nil
}
