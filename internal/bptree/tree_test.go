package bptree

import "testing"

func TestInsertGetAndScan(t *testing.T) {
	tree := New(4)
	for _, key := range []int64{10, 5, 20, 15, 25, 30, 35} {
		if err := tree.Insert(key, uint64(key*10)); err != nil {
			t.Fatalf("insert %d: %v", key, err)
		}
	}

	value, ok := tree.Get(20)
	if !ok || value != 200 {
		t.Fatalf("expected to find key 20 with value 200, got (%d, %v)", value, ok)
	}

	kvs := tree.ScanFrom(15)
	if len(kvs) != 5 {
		t.Fatalf("expected 5 kvs from scan, got %d", len(kvs))
	}
	if kvs[0].Key != 15 || kvs[len(kvs)-1].Key != 35 {
		t.Fatalf("unexpected scan range: %+v", kvs)
	}
}

func TestDuplicateKeysRejected(t *testing.T) {
	tree := New(4)
	if err := tree.Insert(1, 1); err != nil {
		t.Fatalf("first insert failed: %v", err)
	}
	if err := tree.Insert(1, 2); err == nil {
		t.Fatal("expected duplicate key insert to fail")
	}
}
