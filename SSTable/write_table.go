package sstable

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"io/ioutil"
	"os"
	"regexp"
	"sort"
)

// Entry represents a key-value pair in the SSTable
type Entry struct {
	Key   string
	Value []byte
}

// DataBlock is a sorted block of key-value entries
type DataBlock struct {
	Entries []Entry
}

func (db *DataBlock) Encode() []byte {
	buf := new(bytes.Buffer)
	for _, entry := range db.Entries {
		binary.Write(buf, binary.LittleEndian, int32(len(entry.Key)))
		buf.Write([]byte(entry.Key))
		binary.Write(buf, binary.LittleEndian, int32(len(entry.Value)))
		buf.Write(entry.Value)
	}
	return buf.Bytes()
}

// IndexEntry points to the start of a data block in the file
type IndexEntry struct {
	Key    string
	Offset int64
}

// FilterBlock - a basic Bloom filter for demonstration (not optimal)
type FilterBlock struct {
	Filter map[string]bool
}

func (fb *FilterBlock) Add(key string) {
	fb.Filter[key] = true
}

func (fb *FilterBlock) MightContain(key string) bool {
	return fb.Filter[key]
}

// SSTableWriter writes sorted key-value pairs to an SSTable

func getSSTableFilename(index int) string {
	return "sstable_" + fmt.Sprintf("%d", index) + ".sst"
}

func writeSingleSSTable(filename string, entries []Entry, blockSize int) error {
	file, err := os.Create(filename)
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
			file.Write(blockData)
			binary.Write(file, binary.LittleEndian, checksum)
			index = append(index, IndexEntry{Key: block.Entries[0].Key, Offset: offset})
			offset += int64(len(blockData) + 4)
			block.Entries = nil
		}
	}

	indexOffset := offset
	for _, ie := range index {
		binary.Write(file, binary.LittleEndian, int32(len(ie.Key)))
		file.Write([]byte(ie.Key))
		binary.Write(file, binary.LittleEndian, ie.Offset)
	}

	footer := make([]byte, 8)
	binary.LittleEndian.PutUint64(footer, uint64(indexOffset))
	file.Write(footer)

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

func WriteSSTables(baseFilename string, entries []Entry, blockSize int, entriesPerSSTable int) error {
	// Sort entries by key
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].Key < entries[j].Key
	})

	// Get the next available SSTable index
	sstableIndex := getNextSSTableIndex(baseFilename)

	// Write SSTables in chunks
	for i := 0; i < len(entries); i += entriesPerSSTable {
		end := i + entriesPerSSTable
		if end > len(entries) {
			end = len(entries)
		}
		subEntries := entries[i:end]

		filename := fmt.Sprintf("%s_%d.sst", baseFilename, sstableIndex)
		err := writeSingleSSTable(filename, subEntries, blockSize)
		if err != nil {
			return err
		}
		sstableIndex++
	}
	return nil
}

// ReadEntryByKey reads a specific key from the SSTable
// func ReadEntryByKey(filename string, searchKey string) (*Entry, error) {
// 	file, err := os.Open(filename)
// 	if err != nil {
// 		return nil, err
// 	}
// 	defer file.Close()

// 	// Read footer to find index offset
// 	footer := make([]byte, 8)
// 	_, err = file.Seek(-8, io.SeekEnd)
// 	if err != nil {
// 		return nil, err
// 	}
// 	_, err = file.Read(footer)
// 	if err != nil {
// 		return nil, err
// 	}
// 	indexOffset := int64(binary.LittleEndian.Uint64(footer))

// 	// Read index block
// 	_, err = file.Seek(indexOffset, io.SeekStart)
// 	if err != nil {
// 		return nil, err
// 	}

// 	index := []IndexEntry{}
// 	for {
// 		lenBuf := make([]byte, 4)
// 		_, err := file.Read(lenBuf)
// 		if err != nil {
// 			break // end of index
// 		}
// 		keyLen := int32(binary.LittleEndian.Uint32(lenBuf))
// 		keyBuf := make([]byte, keyLen)
// 		_, err = file.Read(keyBuf)
// 		if err != nil {
// 			return nil, err
// 		}
// 		offsetBuf := make([]byte, 8)
// 		_, err = file.Read(offsetBuf)
// 		if err != nil {
// 			return nil, err
// 		}
// 		offset := int64(binary.LittleEndian.Uint64(offsetBuf))
// 		index = append(index, IndexEntry{
// 			Key:    string(keyBuf),
// 			Offset: offset,
// 		})
// 	}

// 	// Binary search for key's block
// 	var blockOffset int64 = -1
// 	for i := 0; i < len(index); i++ {
// 		if index[i].Key == searchKey {
// 			blockOffset = index[i].Offset
// 			break
// 		}
// 		if i+1 < len(index) && index[i].Key < searchKey && searchKey < index[i+1].Key {
// 			blockOffset = index[i].Offset
// 			break
// 		}
// 	}
// 	if blockOffset == -1 {
// 		return nil, errors.New("key not found in index")
// 	}

// 	// Determine block size
// 	var blockSize int64
// 	for i := 0; i < len(index); i++ {
// 		if index[i].Offset == blockOffset {
// 			if i == len(index)-1 {
// 				blockSize = indexOffset - blockOffset
// 			} else {
// 				blockSize = index[i+1].Offset - blockOffset
// 			}
// 			break
// 		}
// 	}

// 	// Read and verify block
// 	_, err = file.Seek(blockOffset, io.SeekStart)
// 	if err != nil {
// 		return nil, err
// 	}
// 	blockData := make([]byte, blockSize-4)
// 	_, err = io.ReadFull(file, blockData)
// 	if err != nil {
// 		return nil, err
// 	}
// 	checksumBuf := make([]byte, 4)
// 	_, err = file.Read(checksumBuf)
// 	if err != nil {
// 		return nil, err
// 	}
// 	storedChecksum := binary.LittleEndian.Uint32(checksumBuf)
// 	if storedChecksum != crc32.ChecksumIEEE(blockData) {
// 		return nil, errors.New("checksum mismatch")
// 	}

// 	// Search for the key inside the block
// 	buf := bytes.NewReader(blockData)
// 	for {
// 		keyLenBuf := make([]byte, 4)
// 		if _, err := buf.Read(keyLenBuf); err == io.EOF {
// 			break
// 		}
// 		keyLen := int32(binary.LittleEndian.Uint32(keyLenBuf))
// 		key := make([]byte, keyLen)
// 		_, err := buf.Read(key)
// 		if err != nil {
// 			return nil, err
// 		}

// 		valLenBuf := make([]byte, 4)
// 		_, err = buf.Read(valLenBuf)
// 		if err != nil {
// 			return nil, err
// 		}
// 		valLen := int32(binary.LittleEndian.Uint32(valLenBuf))
// 		val := make([]byte, valLen)
// 		_, err = buf.Read(val)
// 		if err != nil {
// 			return nil, err
// 		}

// 		if string(key) == searchKey {
// 			return &Entry{
// 				Key:   string(key),
// 				Value: val,
// 			}, nil
// 		}
// 	}
// 	return nil, errors.New("key not found in data block")
// }

// func ReadEntryFromAllSSTables(key string, sstableFiles []string) (*Entry, string, error) {
// 	for _, file := range sstableFiles {
// 		entry, err := ReadEntryByKey(file, key)
// 		if err == nil && entry != nil {
// 			return entry, file, nil
// 		}
// 	}
// 	return nil, "", fmt.Errorf("key '%s' not found in any SSTable", key)
// }
