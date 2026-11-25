package txn

import (
	"bytes"
	"go-lsm/kv"
	"sort"
)

// PendingWritesIterator iterates over the key/value pairs of a Readwrite Transaction that is yet to be committed.
type PendingWritesIterator struct {
	keyValuePairs     []kv.RawKeyValuePair
	index             int
	beginTimestamp    uint64
	inclusiveKeyRange kv.InclusiveKeyRange[kv.RawKey]
}

// NewPendingWritesIterator creates a new instance of PendingWritesIterator.
// It involves the following:
// 1) Clone all the key/value pairs present in the kv.Batch, and sorts them by key in increasing order.
// For pairs with the same key, the one written later (the one with the larger sequence) is sorted first,
// so the latest write of a key is always encountered before its older writes.
// 2) Sort allows a binary search in the first seek operation.
// 3) Seek to a key greater than or equal to the starting key of the keyRange.
// Clone is done to ensure that iterator is not impacted even if the kv.Batch is modified after creating an instance of
// PendingWritesIterator.
func NewPendingWritesIterator(batch *kv.Batch, beginTimestamp uint64, keyRange kv.InclusiveKeyRange[kv.RawKey]) *PendingWritesIterator {
	keyValuePairs := batch.CloneKeyValuePairs()
	sort.Slice(keyValuePairs, func(i, j int) bool {
		comparison := bytes.Compare(keyValuePairs[i].Key(), keyValuePairs[j].Key())
		if comparison != 0 {
			return comparison < 0
		}
		return keyValuePairs[i].Sequence() > keyValuePairs[j].Sequence()
	})
	iterator := &PendingWritesIterator{
		keyValuePairs:     keyValuePairs,
		index:             0,
		beginTimestamp:    beginTimestamp,
		inclusiveKeyRange: keyRange,
	}
	iterator.seek(keyRange.Start())
	return iterator
}

// Key returns the key at the current index of the iterator.
// It is important to understand that PendingWritesIterator iterates over key/value pairs present in the kv.Batch that is a part
// of a Readwrite transaction which is yet to be committed. This means the transaction does not have a commit-timestamp yet.
// Hence, PendingWritesIterator technically iterates over raw keys.
// To return an instance of kv.Key from the Key() method, it simply uses the begin-timestamp of the transaction along with the
// raw key.
func (iterator *PendingWritesIterator) Key() kv.Key {
	pair := iterator.keyValuePairs[iterator.index]
	return kv.NewKey(pair.Key(), iterator.beginTimestamp)
}

// Value returns the value at the current index of the iterator.
func (iterator *PendingWritesIterator) Value() kv.Value {
	return iterator.keyValuePairs[iterator.index].Value()
}

// Next moves the iterator ahead to the next distinct raw key.
// Older writes of the key being left behind are skipped, so every raw key is exposed at most once,
// through its latest write.
func (iterator *PendingWritesIterator) Next() error {
	currentKey := iterator.keyValuePairs[iterator.index].Key()
	iterator.index++
	for iterator.index < len(iterator.keyValuePairs) &&
		bytes.Equal(iterator.keyValuePairs[iterator.index].Key(), currentKey) {
		iterator.index++
	}
	return nil
}

// IsValid returns true of the index of the iterator is less than the total number of key/value pairs,
// and the current raw key is less than or equal to the end key of the keyRange.
func (iterator *PendingWritesIterator) IsValid() bool {
	return iterator.index < len(iterator.keyValuePairs) &&
		kv.RawKey(iterator.Key().RawBytes()).IsLessThanOrEqualTo(iterator.inclusiveKeyRange.End())
}

// Close does nothing.
func (iterator *PendingWritesIterator) Close() {}

// seek seeks to a key greater than or equal to the given key.
// seek leverages binary search because keyValuePairs are already sorted.
// When the given key has multiple writes, seek lands on its first pair, which is the latest write because
// same-key pairs are ordered by descending sequence. The older writes of the key are skipped later in Next.
func (iterator *PendingWritesIterator) seek(key []byte) {
	iterator.index = len(iterator.keyValuePairs)
	low, high := 0, len(iterator.keyValuePairs)-1

	for low <= high {
		mid := low + (high-low)/2
		if bytes.Compare(iterator.keyValuePairs[mid].Key(), key) >= 0 {
			iterator.index = mid //possible index
			high = mid - 1
		} else {
			low = mid + 1
		}
	}
}
