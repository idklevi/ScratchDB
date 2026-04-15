package bptree

import (
	"fmt"
	"sort"
)

const defaultOrder = 4

type Tree struct {
	order int
	root  *node
}

type node struct {
	leaf     bool
	keys     []int64
	values   []uint64
	children []*node
	next     *node
}

type KV struct {
	Key   int64
	Value uint64
}

func New(order int) *Tree {
	if order < 3 {
		order = defaultOrder
	}
	return &Tree{
		order: order,
		root:  &node{leaf: true},
	}
}

func (t *Tree) Insert(key int64, value uint64) error {
	if t.root == nil {
		t.root = &node{leaf: true}
	}

	if len(t.root.keys) >= t.maxKeys() {
		oldRoot := t.root
		t.root = &node{
			leaf:     false,
			children: []*node{oldRoot},
		}
		t.splitChild(t.root, 0)
	}

	return t.insertNonFull(t.root, key, value)
}

func (t *Tree) Get(key int64) (uint64, bool) {
	leaf := t.findLeaf(key)
	if leaf == nil {
		return 0, false
	}

	index := sort.Search(len(leaf.keys), func(i int) bool {
		return leaf.keys[i] >= key
	})
	if index < len(leaf.keys) && leaf.keys[index] == key {
		return leaf.values[index], true
	}
	return 0, false
}

func (t *Tree) ScanFrom(minKey int64) []KV {
	leaf := t.findLeaf(minKey)
	if leaf == nil {
		return nil
	}

	index := sort.Search(len(leaf.keys), func(i int) bool {
		return leaf.keys[i] >= minKey
	})

	var result []KV
	for current := leaf; current != nil; current = current.next {
		start := 0
		if current == leaf {
			start = index
		}
		for i := start; i < len(current.keys); i++ {
			result = append(result, KV{Key: current.keys[i], Value: current.values[i]})
		}
	}
	return result
}

func (t *Tree) All() []KV {
	if t.root == nil {
		return nil
	}

	current := t.root
	for current != nil && !current.leaf {
		current = current.children[0]
	}

	var result []KV
	for ; current != nil; current = current.next {
		for i := range current.keys {
			result = append(result, KV{Key: current.keys[i], Value: current.values[i]})
		}
	}
	return result
}

func (t *Tree) insertNonFull(current *node, key int64, value uint64) error {
	if current.leaf {
		index := sort.Search(len(current.keys), func(i int) bool {
			return current.keys[i] >= key
		})

		if index < len(current.keys) && current.keys[index] == key {
			return fmt.Errorf("duplicate key %d", key)
		}

		current.keys = insertInt64(current.keys, index, key)
		current.values = insertUint64(current.values, index, value)
		return nil
	}

	index := sort.Search(len(current.keys), func(i int) bool {
		return key < current.keys[i]
	})

	child := current.children[index]
	if len(child.keys) >= t.maxKeys() {
		t.splitChild(current, index)
		if key >= current.keys[index] {
			index++
		}
	}

	return t.insertNonFull(current.children[index], key, value)
}

func (t *Tree) splitChild(parent *node, childIndex int) {
	child := parent.children[childIndex]
	mid := len(child.keys) / 2

	sibling := &node{leaf: child.leaf}
	if child.leaf {
		sibling.keys = append(sibling.keys, child.keys[mid:]...)
		sibling.values = append(sibling.values, child.values[mid:]...)

		child.keys = child.keys[:mid]
		child.values = child.values[:mid]

		sibling.next = child.next
		child.next = sibling

		parent.keys = insertInt64(parent.keys, childIndex, sibling.keys[0])
		parent.children = insertNode(parent.children, childIndex+1, sibling)
		return
	}

	separator := child.keys[mid]
	sibling.keys = append(sibling.keys, child.keys[mid+1:]...)
	sibling.children = append(sibling.children, child.children[mid+1:]...)

	child.keys = child.keys[:mid]
	child.children = child.children[:mid+1]

	parent.keys = insertInt64(parent.keys, childIndex, separator)
	parent.children = insertNode(parent.children, childIndex+1, sibling)
}

func (t *Tree) findLeaf(key int64) *node {
	if t.root == nil {
		return nil
	}

	current := t.root
	for !current.leaf {
		index := sort.Search(len(current.keys), func(i int) bool {
			return key < current.keys[i]
		})
		current = current.children[index]
	}
	return current
}

func (t *Tree) maxKeys() int {
	return t.order - 1
}

func insertInt64(values []int64, index int, value int64) []int64 {
	values = append(values, 0)
	copy(values[index+1:], values[index:])
	values[index] = value
	return values
}

func insertUint64(values []uint64, index int, value uint64) []uint64 {
	values = append(values, 0)
	copy(values[index+1:], values[index:])
	values[index] = value
	return values
}

func insertNode(values []*node, index int, value *node) []*node {
	values = append(values, nil)
	copy(values[index+1:], values[index:])
	values[index] = value
	return values
}
