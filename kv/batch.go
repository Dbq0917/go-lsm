package kv

import (
	"bytes"
	"errors"
)

// RawKeyValuePair represents the key/value pair with Kind.
type RawKeyValuePair struct {
	key   []byte
	value Value
	kind  Kind
	// sequence is the position of the pair in its kv.Batch, starting from zero.
	// It makes two pairs with the same key comparable: the pair with the larger
	// sequence is the later write and must win. Without it, sorting only by key
	// leaves the relative order of same-key pairs unspecified.
	sequence int
}

// Key returns the key.
func (kv RawKeyValuePair) Key() []byte {
	return kv.key
}

// Value returns the value.
func (kv RawKeyValuePair) Value() Value {
	return kv.value
}

// Sequence returns the position of the pair in its kv.Batch.
func (kv RawKeyValuePair) Sequence() int {
	return kv.sequence
}

var DuplicateKeyInBatchErr = errors.New("batch already contains the key")

// Batch is a collection of RawKeyValuePair.
// Batch is typically used in a transaction (txn.Transaction). All the inserts within a transaction (read/write transaction)
// are batched and finally the entire Batch is committed.
type Batch struct {
	pairs []RawKeyValuePair
}

// NewBatch creates an empty Batch.
func NewBatch() *Batch {
	return &Batch{}
}

// Put puts the key/value pair in Batch.
// Returns DuplicateKeyInBatchErr if the key is already present in the Batch.
func (batch *Batch) Put(key, value []byte) error {
	if batch.Contains(key) {
		return DuplicateKeyInBatchErr
	}
	batch.pairs = append(batch.pairs, RawKeyValuePair{
		key:      key,
		value:    NewValue(value),
		kind:     EntryKindPut,
		sequence: len(batch.pairs),
	})
	return nil
}

// Delete is modeled as an append operation. It results in another RawKeyValuePair in batch with kind as EntryKindDelete.
func (batch *Batch) Delete(key []byte) {
	batch.pairs = append(batch.pairs, RawKeyValuePair{
		key:      key,
		value:    EmptyValue,
		kind:     EntryKindDelete,
		sequence: len(batch.pairs),
	})
}

// Get returns the Value for the given key if found.
// When the key was written more than once in the Batch, the value of the last
// write is returned, matching write-write semantics within a single transaction.
func (batch *Batch) Get(key []byte) (Value, bool) {
	var value Value
	found := false
	for _, pair := range batch.pairs {
		if bytes.Equal(pair.key, key) {
			value = pair.value
			found = true
		}
	}
	return value, found
}

// Contains returns true of the key is present in Batch.
func (batch *Batch) Contains(key []byte) bool {
	_, ok := batch.Get(key)
	return ok
}

// IsEmpty returns true if the Batch is empty.
func (batch *Batch) IsEmpty() bool {
	return len(batch.pairs) == 0
}

// Length returns the number of RawKeyValuePair(s) in the Batch.
func (batch *Batch) Length() int {
	return len(batch.pairs)
}

// CloneKeyValuePairs clones the RawKeyValuePair(s) present in the Batch.
func (batch *Batch) CloneKeyValuePairs() []RawKeyValuePair {
	keyValuePairs := make([]RawKeyValuePair, 0, batch.Length())
	keyValuePairs = append(keyValuePairs, batch.pairs...)
	return keyValuePairs
}
