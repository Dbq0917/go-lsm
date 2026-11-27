package manifest

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"go-lsm/compact/meta"
	"io"
	"unsafe"
)

const (
	idSize        = unsafe.Sizeof(uint64(0))
	eventTypeSize = unsafe.Sizeof(uint8(0))
)

// Event types.
const (
	MemtableCreatedEventType uint8 = iota
	SSTableFlushedEventType  uint8 = 1
	CompactionDoneEventType  uint8 = 2
	SnapshotEventType        uint8 = 3
)

// StorageStateSnapshotEventType is kept as an alias for SnapshotEventType.
const StorageStateSnapshotEventType = SnapshotEventType

// Event represents a manifest event.
type Event interface {
	encode() ([]byte, error)
	EventType() uint8
}

// MemtableCreated defines a new memtable event.
type MemtableCreated struct {
	MemtableId uint64
}

// SSTableFlushed defines an SSTable flushed (to L0) event. (Memtable flushed to SSTable).
type SSTableFlushed struct {
	SsTableId uint64
}

// CompactionDone defines a compaction done event.
type CompactionDone struct {
	NewSSTableIds []uint64
	Description   meta.SimpleLeveledCompactionDescription
}

// StorageStateSnapshot defines the complete structural state of the storage at a point in time.
// It is written when the manifest is compacted, replacing all the preceding events.
type StorageStateSnapshot struct {
	// MemtableIds contains all the live memtables, from oldest to latest.
	MemtableIds []uint64
	// L0SSTableIds contains the live level-0 SSTables, from oldest to latest.
	L0SSTableIds []uint64
	// Levels contains the live SSTables for levels 1..N (index 0 is level 1).
	Levels [][]uint64
	// MaxId is the largest id ever allocated. It prevents ids from being reused after compaction.
	MaxId uint64
}

// Snapshot is an alias for StorageStateSnapshot.
type Snapshot = StorageStateSnapshot

// NewMemtableCreated creates a new MemtableCreated event.
func NewMemtableCreated(memtableId uint64) *MemtableCreated {
	return &MemtableCreated{MemtableId: memtableId}
}

// encode encodes MemtableCreated to byte slice.
/*
 -----------------------------------------------
| 1 byte event type | 8 bytes for the MemtableId |
 -----------------------------------------------
*/
func (memtableCreated *MemtableCreated) encode() ([]byte, error) {
	buffer := make([]byte, eventTypeSize+idSize)
	buffer[0] = MemtableCreatedEventType
	binary.LittleEndian.PutUint64(buffer[1:], memtableCreated.MemtableId)
	return buffer, nil
}

// EventType returns the event type MemtableCreatedEventType.
func (memtableCreated *MemtableCreated) EventType() uint8 {
	return MemtableCreatedEventType
}

// decodeMemtableCreated decodes the MemtableCreated event from the byte slice.
// The buffer is a slice containing MemtableId.
func decodeMemtableCreated(buffer []byte) (*MemtableCreated, int) {
	if len(buffer) < int(idSize) {
		return nil, 0
	}
	return NewMemtableCreated(binary.LittleEndian.Uint64(buffer[:])), int(idSize)
}

// NewSSTableFlushed creates a new SSTableFlushed event.
func NewSSTableFlushed(ssTableId uint64) *SSTableFlushed {
	return &SSTableFlushed{SsTableId: ssTableId}
}

// encode encodes SSTableFlushed to byte slice.
/*
 -----------------------------------------------
| 1 byte event type | 8 bytes for the SsTableId |
 -----------------------------------------------
*/
func (ssTableFlushed *SSTableFlushed) encode() ([]byte, error) {
	buffer := make([]byte, eventTypeSize+idSize)
	buffer[0] = SSTableFlushedEventType
	binary.LittleEndian.PutUint64(buffer[1:], ssTableFlushed.SsTableId)
	return buffer, nil
}

// EventType returns the event type SSTableFlushedEventType.
func (ssTableFlushed *SSTableFlushed) EventType() uint8 {
	return SSTableFlushedEventType
}

// decodeSSTableFlushed decodes the SSTableFlushed event from the byte slice.
// The buffer is a slice containing SsTableId.
func decodeSSTableFlushed(buffer []byte) (*SSTableFlushed, int) {
	if len(buffer) < int(idSize) {
		return nil, 0
	}
	return NewSSTableFlushed(binary.LittleEndian.Uint64(buffer[:])), int(idSize)
}

// NewCompactionDone creates a new CompactionDone event.
func NewCompactionDone(newSSTableIds []uint64, description meta.SimpleLeveledCompactionDescription) *CompactionDone {
	return &CompactionDone{
		NewSSTableIds: newSSTableIds,
		Description:   description,
	}
}

// encode encodes CompactionDone to byte slice.
func (compactionDone *CompactionDone) encode() ([]byte, error) {
	buffer := bytes.Buffer{}
	err := gob.NewEncoder(&buffer).Encode(compactionDone)
	if err != nil {
		return nil, err
	}
	buffered := buffer.Bytes()
	encoded := make([]byte, int(eventTypeSize)+len(buffered))
	encoded[0] = CompactionDoneEventType
	copy(encoded[1:], buffered)

	return encoded, nil
}

// EventType returns the event type CompactionDoneEventType.
func (compactionDone *CompactionDone) EventType() uint8 {
	return CompactionDoneEventType
}

// decodeCompactionDone decodes the CompactionDone event from the byte slice.
func decodeCompactionDone(buffer []byte) (*CompactionDone, int) {
	compactionDone := &CompactionDone{}
	reader := &byteCountingReader{reader: bytes.NewReader(buffer)}
	err := gob.NewDecoder(reader).Decode(compactionDone)
	if err != nil {
		return nil, 0
	}
	return compactionDone, int(reader.count)
}

// NewStorageStateSnapshot creates a new snapshot event.
func NewStorageStateSnapshot(memtableIds []uint64, l0SSTableIds []uint64, levels [][]uint64, maxId uint64) *StorageStateSnapshot {
	return &StorageStateSnapshot{
		MemtableIds:  memtableIds,
		L0SSTableIds: l0SSTableIds,
		Levels:       levels,
		MaxId:        maxId,
	}
}

// encode encodes StorageStateSnapshot to a byte slice.
/*
 -----------------------------------------------------------
| 1 byte event type | 8 bytes payload size | gob encoded payload |
 -----------------------------------------------------------
*/
func (snapshot *StorageStateSnapshot) encode() ([]byte, error) {
	buffer := bytes.Buffer{}
	if err := gob.NewEncoder(&buffer).Encode(snapshot); err != nil {
		return nil, err
	}
	buffered := buffer.Bytes()
	encoded := make([]byte, int(eventTypeSize+idSize)+len(buffered))
	encoded[0] = SnapshotEventType
	binary.LittleEndian.PutUint64(encoded[eventTypeSize:], uint64(len(buffered)))
	copy(encoded[eventTypeSize+idSize:], buffered)
	return encoded, nil
}

// EventType returns the event type SnapshotEventType.
func (snapshot *StorageStateSnapshot) EventType() uint8 {
	return SnapshotEventType
}

// decodeStorageStateSnapshot decodes the StorageStateSnapshot event from the byte slice.
func decodeStorageStateSnapshot(buffer []byte) (*StorageStateSnapshot, int) {
	if len(buffer) < int(idSize) {
		return nil, 0
	}
	payloadSize := binary.LittleEndian.Uint64(buffer[:idSize])
	if payloadSize > uint64(len(buffer)-int(idSize)) {
		return nil, 0
	}

	snapshot := &StorageStateSnapshot{}
	payload := buffer[idSize : uint64(idSize)+payloadSize]
	if err := gob.NewDecoder(bytes.NewReader(payload)).Decode(snapshot); err != nil {
		return nil, 0
	}
	return snapshot, int(idSize) + int(payloadSize)
}

// decodeEventsFrom decodes all the events from the Manifest file. The passed buffer is the whole file.
func decodeEventsFrom(buffer []byte) []Event {
	events, _ := decodeEventsFromWithValidLength(buffer)
	return events
}

// decodeEventsFromWithValidLength decodes complete events from the buffer and
// returns the number of bytes covered by them. A partial or unknown tail is not
// treated as an event.
func decodeEventsFromWithValidLength(buffer []byte) ([]Event, int) {
	var events []Event
	offset := 0
	for offset < len(buffer) {
		remaining := buffer[offset:]
		eventType := remaining[0]
		var event Event
		var decodedSize int
		switch eventType {
		case MemtableCreatedEventType:
			if len(remaining) < int(eventTypeSize+idSize) {
				return events, offset
			}
			memtableCreated, n := decodeMemtableCreated(remaining[eventTypeSize:])
			if n == 0 {
				return events, offset
			}
			event, decodedSize = memtableCreated, n+int(eventTypeSize)
		case SSTableFlushedEventType:
			if len(remaining) < int(eventTypeSize+idSize) {
				return events, offset
			}
			ssTableFlushed, n := decodeSSTableFlushed(remaining[eventTypeSize:])
			if n == 0 {
				return events, offset
			}
			event, decodedSize = ssTableFlushed, n+int(eventTypeSize)
		case CompactionDoneEventType:
			compactionDone, n := decodeCompactionDone(remaining[eventTypeSize:])
			if n == 0 {
				return events, offset
			}
			event, decodedSize = compactionDone, n+int(eventTypeSize)
		case SnapshotEventType:
			snapshot, n := decodeStorageStateSnapshot(remaining[eventTypeSize:])
			if n == 0 {
				return events, offset
			}
			event, decodedSize = snapshot, n+int(eventTypeSize)
		default:
			// An unknown event type can only be the result of a partial or corrupt tail.
			return events, offset
		}
		events = append(events, event)
		offset += decodedSize
	}
	return events, offset
}

// byteCountingReader counts the number of bytes read while encapsulates a reader.
// It is mainly used in decoding of CompactionDoneEventType.
type byteCountingReader struct {
	reader io.Reader
	count  int64
}

// Read reads from the given byte slice.
func (reader *byteCountingReader) Read(p []byte) (n int, err error) {
	n, err = reader.reader.Read(p)
	reader.count += int64(n)
	return
}

// ReadByte makes byteCountingReader implement io.ByteReader. Without this,
// gob.NewDecoder wraps the reader in a bufio.Reader, which can read past the
// end of one gob message and make the byte count useless for finding the next
// manifest event.
func (reader *byteCountingReader) ReadByte() (byte, error) {
	var buffer [1]byte
	n, err := reader.Read(buffer[:])
	if n == 1 {
		return buffer[0], nil
	}
	if err == nil {
		err = io.ErrUnexpectedEOF
	}
	return 0, err
}
