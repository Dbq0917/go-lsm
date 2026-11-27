package manifest

import (
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
)

const (
	manifestFileName = "manifest"
	// DefaultCompactionThreshold is the number of events after which the manifest is compacted.
	DefaultCompactionThreshold = 100
)

// Manifest records different events in the system.
// The events are described by the Event interface.
type Manifest struct {
	path                string
	file                *os.File
	writeLock           sync.RWMutex
	stopChannel         chan struct{}
	eventCount          int
	compactionThreshold int
}

// CreateNewOrRecoverFrom either creates a new Manifest or recovers from an existing manifest file.
func CreateNewOrRecoverFrom(directoryPath string) (*Manifest, []Event, error) {
	return createNewOrRecoverFrom(directoryPath, DefaultCompactionThreshold)
}

// CreateNewOrRecoverFromWithCompactionThreshold either creates a new Manifest or recovers from an existing
// manifest file, compacting it after the given number of events. A non-positive threshold disables automatic
// compaction; Compact can still be called explicitly.
func CreateNewOrRecoverFromWithCompactionThreshold(directoryPath string, compactionThreshold int) (*Manifest, []Event, error) {
	return createNewOrRecoverFrom(directoryPath, compactionThreshold)
}

func createNewOrRecoverFrom(directoryPath string, compactionThreshold int) (*Manifest, []Event, error) {
	path := filepath.Join(directoryPath, manifestFileName)
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0666)
	if err != nil {
		return nil, nil, err
	}

	// A temporary file is only a candidate snapshot. It is safe to discard it if
	// the process stopped before the atomic rename or after it.
	_ = os.Remove(compactionTempPath(path))

	manifest := &Manifest{
		path:                path,
		file:                file,
		stopChannel:         make(chan struct{}),
		compactionThreshold: compactionThreshold,
	}
	events, err := manifest.attemptRecovery()
	if err != nil {
		_ = file.Close()
		return nil, nil, err
	}
	manifest.eventCount = len(events)

	if manifest.shouldCompactLocked() {
		if _, err := manifest.compactLocked(); err != nil {
			// The old manifest is still valid when compaction fails before the
			// rename, so recovery can continue from it.
			slog.Warn(fmt.Sprintf("error while compacting manifest during recovery: %v", err))
		} else {
			events, err = manifest.attemptRecovery()
			if err != nil {
				_ = manifest.file.Close()
				return nil, nil, err
			}
		}
	}
	return manifest, events, nil
}

// Add adds the event to the manifest file.
func (manifest *Manifest) Add(event Event) error {
	manifest.writeLock.Lock()
	defer manifest.writeLock.Unlock()

	buf, err := event.encode()
	if err != nil {
		slog.Warn(fmt.Sprintf("error while serializing event: %v", err))
		return err
	}
	if err = writeAll(manifest.file, buf); err != nil {
		slog.Warn(fmt.Sprintf("error while writing event: %v", err))
		return err
	}
	if err = manifest.file.Sync(); err != nil {
		slog.Warn(fmt.Sprintf("error while syncing event: %v", err))
		return err
	}
	manifest.eventCount++

	if manifest.shouldCompactLocked() {
		if _, err = manifest.compactLocked(); err != nil {
			slog.Warn(fmt.Sprintf("error while compacting manifest: %v", err))
			return err
		}
	}
	return nil
}

// Compact rewrites the manifest as a single snapshot of its current state.
// Events added concurrently with compaction are serialized with it and are
// appended after the snapshot.
func (manifest *Manifest) Compact() error {
	manifest.writeLock.Lock()
	defer manifest.writeLock.Unlock()

	_, err := manifest.compactLocked()
	return err
}

// Close closes the manifest file.
func (manifest *Manifest) Close() error {
	manifest.writeLock.Lock()
	defer manifest.writeLock.Unlock()

	return manifest.file.Close()
}

// attemptRecovery attempts recovery of events from the Manifest file.
// This implementation reads the whole file and passes the byte slice to decodeEventsFrom() method.
func (manifest *Manifest) attemptRecovery() ([]Event, error) {
	if _, err := manifest.file.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}
	bytes, err := io.ReadAll(manifest.file)
	if err != nil {
		return nil, err
	}
	events, validLength := decodeEventsFromWithValidLength(bytes)
	if validLength < len(bytes) {
		// A process crash can leave a partially written final event. Drop only
		// that invalid tail so future appends remain decodable.
		if err := manifest.file.Truncate(int64(validLength)); err != nil {
			return nil, err
		}
		if _, err := manifest.file.Seek(0, io.SeekEnd); err != nil {
			return nil, err
		}
		if err := manifest.file.Sync(); err != nil {
			return nil, err
		}
	}
	return events, nil
}

func (manifest *Manifest) shouldCompactLocked() bool {
	return manifest.compactionThreshold > 0 && manifest.eventCount >= manifest.compactionThreshold
}

// compactLocked replaces the manifest with one snapshot event. The caller must
// hold writeLock.
//
// The snapshot is first written and synced to a temporary file in the same
// directory. Renaming that file over the manifest is atomic: after a crash the
// recovery sees either the complete old manifest or the complete new manifest.
// Because Add uses the same lock, an appended event is always placed after the
// snapshot and never interleaves with the replacement.
func (manifest *Manifest) compactLocked() (*StorageStateSnapshot, error) {
	events, err := manifest.attemptRecovery()
	if err != nil {
		return nil, err
	}

	snapshot := snapshotFromEvents(events)
	encoded, err := snapshot.encode()
	if err != nil {
		return nil, err
	}

	tempPath := compactionTempPath(manifest.path)
	tempFile, err := os.OpenFile(tempPath, os.O_CREATE|os.O_TRUNC|os.O_RDWR|os.O_APPEND, 0666)
	if err != nil {
		return nil, err
	}
	removeTempFile := true
	defer func() {
		if removeTempFile {
			_ = tempFile.Close()
			_ = os.Remove(tempPath)
		}
	}()

	if err = writeAll(tempFile, encoded); err != nil {
		return nil, err
	}
	if err = tempFile.Sync(); err != nil {
		return nil, err
	}
	if err = os.Rename(tempPath, manifest.path); err != nil {
		return nil, err
	}
	removeTempFile = false

	// Persist the rename itself. Once the rename has succeeded, switch to the
	// new file even if syncing the directory fails; continuing to append to the
	// unlinked old file would make those events invisible.
	directorySyncErr := syncDirectory(filepath.Dir(manifest.path))
	oldFile := manifest.file
	manifest.file = tempFile
	manifest.eventCount = 1
	_ = oldFile.Close()

	return snapshot, directorySyncErr
}

func compactionTempPath(path string) string {
	return path + ".compaction.tmp"
}

func writeAll(file *os.File, data []byte) error {
	for len(data) > 0 {
		written, err := file.Write(data)
		if err != nil {
			return err
		}
		if written == 0 {
			return io.ErrShortWrite
		}
		data = data[written:]
	}
	return nil
}

func syncDirectory(path string) error {
	directory, err := os.Open(path)
	if err != nil {
		return err
	}
	defer func() {
		_ = directory.Close()
	}()
	return directory.Sync()
}
