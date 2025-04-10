package internal

import (
	"math/rand"
	"time"
)

const maxLevel = 6
const p = 0.5
const memtableLimit = 1000 // max entries before flush

type Node struct {
	key     string
	value   string
	forward []*Node
}

type SkipList struct {
	header *Node
	level  int
	size   int
}

func newNode(level int, key, value string) *Node {
	return &Node{
		key:     key,
		value:   value,
		forward: make([]*Node, level+1),
	}
}

func NewSkipList() *SkipList {
	rand.Seed(time.Now().UnixNano())
	return &SkipList{
		header: newNode(maxLevel, "", ""),
		level:  0,
		size:   0,
	}
}

func (sl *SkipList) randomLevel() int {
	lvl := 0
	for rand.Float64() < p && lvl < maxLevel {
		lvl++
	}
	return lvl
}

func (sl *SkipList) Insert(key, value string) {
	update := make([]*Node, maxLevel+1)
	x := sl.header

	for i := sl.level; i >= 0; i-- {
		for x.forward[i] != nil && x.forward[i].key < key {
			x = x.forward[i]
		}
		update[i] = x
	}
	x = x.forward[0]

	if x != nil && x.key == key {
		x.value = value
		return
	}

	lvl := sl.randomLevel()
	if lvl > sl.level {
		for i := sl.level + 1; i <= lvl; i++ {
			update[i] = sl.header
		}
		sl.level = lvl
	}

	newNode := newNode(lvl, key, value)
	for i := 0; i <= lvl; i++ {
		newNode.forward[i] = update[i].forward[i]
		update[i].forward[i] = newNode
	}

	sl.size++
}

func (sl *SkipList) Search(key string) (string, bool) {
	x := sl.header
	for i := sl.level; i >= 0; i-- {
		for x.forward[i] != nil && x.forward[i].key < key {
			x = x.forward[i]
		}
	}
	x = x.forward[0]
	if x != nil && x.key == key {
		return x.value, true
	}
	return "", false
}

func (sl *SkipList) Delete(key string) bool {
	update := make([]*Node, maxLevel+1)
	x := sl.header

	for i := sl.level; i >= 0; i-- {
		for x.forward[i] != nil && x.forward[i].key < key {
			x = x.forward[i]
		}
		update[i] = x
	}

	x = x.forward[0]
	if x == nil || x.key != key {
		return false
	}

	for i := 0; i <= sl.level; i++ {
		if update[i].forward[i] != x {
			break
		}
		update[i].forward[i] = x.forward[i]
	}
	sl.size--
	return true
}

func (sl *SkipList) Reset() {
	sl.header = newNode(maxLevel, "", "")
	sl.level = 0
	sl.size = 0
}

func (sl *SkipList) IsFull() bool {
	return sl.size >= memtableLimit
}

func (sl *SkipList) GetAll() map[string]string {
	result := make(map[string]string)
	x := sl.header.forward[0]
	for x != nil {
		result[x.key] = x.value
		x = x.forward[0]
	}
	return result
}
