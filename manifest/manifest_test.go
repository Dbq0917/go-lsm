package manifest

import (
	"go-lsm/compact/meta"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCreateANewManifestWithNewMemtableCreatedEvent(t *testing.T) {
	manifestDirectoryPath := filepath.Join(".", "TestCreateANewManifestWithNewMemtableCreatedEvent")
	assert.Nil(t, os.MkdirAll(manifestDirectoryPath, os.ModePerm))

	manifest, _, err := CreateNewOrRecoverFrom(manifestDirectoryPath)
	defer func() {
		_ = os.RemoveAll(manifestDirectoryPath)
	}()

	assert.Nil(t, err)
	assert.Nil(t, manifest.Add(NewMemtableCreated(10)))
}

func TestCreateANewManifestWithNewSSTableFlushedEvent(t *testing.T) {
	manifestDirectoryPath := filepath.Join(".", "TestCreateANewManifestWithNewSSTableFlushedEvent")
	assert.Nil(t, os.MkdirAll(manifestDirectoryPath, os.ModePerm))

	manifest, _, err := CreateNewOrRecoverFrom(manifestDirectoryPath)
	defer func() {
		_ = os.RemoveAll(manifestDirectoryPath)
	}()

	assert.Nil(t, err)
	assert.Nil(t, manifest.Add(NewSSTableFlushed(10)))
}

func TestRecoversAnExistingManifest(t *testing.T) {
	manifestDirectoryPath := filepath.Join(".", "TestRecoversAnExistingManifest")
	assert.Nil(t, os.MkdirAll(manifestDirectoryPath, os.ModePerm))

	manifest, _, err := CreateNewOrRecoverFrom(manifestDirectoryPath)
	defer func() {
		_ = os.RemoveAll(manifestDirectoryPath)
	}()

	assert.Nil(t, err)
	assert.Nil(t, manifest.Add(NewMemtableCreated(10)))
	assert.Nil(t, manifest.Add(NewMemtableCreated(20)))
	assert.Nil(t, manifest.Add(NewSSTableFlushed(10)))

	upperLevel := -1
	lowerLevel := 1
	upperLevelSSTableIds := []uint64{20, 30}
	lowerLevelSSTableIds := []uint64{50, 60}

	compactionDone := NewCompactionDone([]uint64{10, 11}, meta.SimpleLeveledCompactionDescription{
		UpperLevel:           upperLevel,
		LowerLevel:           lowerLevel,
		UpperLevelSSTableIds: upperLevelSSTableIds,
		LowerLevelSSTableIds: lowerLevelSSTableIds,
	})
	assert.Nil(t, manifest.Add(compactionDone))

	_, events, err := CreateNewOrRecoverFrom(manifestDirectoryPath)
	assert.Nil(t, err)

	assert.Equal(t, 4, len(events))
	assert.Equal(t, uint64(10), events[0].(*MemtableCreated).MemtableId)
	assert.Equal(t, uint64(20), events[1].(*MemtableCreated).MemtableId)
	assert.Equal(t, uint64(10), events[2].(*SSTableFlushed).SsTableId)

	assert.Equal(t, []uint64{10, 11}, events[3].(*CompactionDone).NewSSTableIds)
	assert.Equal(t, -1, events[3].(*CompactionDone).Description.UpperLevel)
	assert.Equal(t, 1, events[3].(*CompactionDone).Description.LowerLevel)
	assert.Equal(t, []uint64{20, 30}, events[3].(*CompactionDone).Description.UpperLevelSSTableIds)
	assert.Equal(t, []uint64{50, 60}, events[3].(*CompactionDone).Description.LowerLevelSSTableIds)
}

func TestCompactRewritesTheManifestAsOneSnapshot(t *testing.T) {
	manifestDirectoryPath := filepath.Join(".", "TestCompactRewritesTheManifestAsOneSnapshot")
	_ = os.RemoveAll(manifestDirectoryPath)
	assert.Nil(t, os.MkdirAll(manifestDirectoryPath, os.ModePerm))

	manifest, _, err := CreateNewOrRecoverFromWithCompactionThreshold(manifestDirectoryPath, 0)
	assert.Nil(t, err)
	defer func() {
		_ = manifest.Close()
		_ = os.RemoveAll(manifestDirectoryPath)
	}()

	assert.Nil(t, manifest.Add(NewMemtableCreated(10)))
	assert.Nil(t, manifest.Add(NewMemtableCreated(11)))
	assert.Nil(t, manifest.Add(NewSSTableFlushed(10)))
	assert.Nil(t, manifest.Add(NewSSTableFlushed(11)))
	assert.Nil(t, manifest.Add(NewCompactionDone([]uint64{20, 21}, meta.SimpleLeveledCompactionDescription{
		UpperLevel:           -1,
		LowerLevel:           1,
		UpperLevelSSTableIds: []uint64{10, 11},
	})))
	assert.Nil(t, manifest.Add(NewMemtableCreated(22)))
	assert.Nil(t, manifest.Add(NewCompactionDone([]uint64{30}, meta.SimpleLeveledCompactionDescription{
		UpperLevel:           1,
		LowerLevel:           2,
		UpperLevelSSTableIds: []uint64{20, 21},
	})))
	assert.Nil(t, manifest.Add(NewMemtableCreated(23)))

	assert.Nil(t, manifest.Compact())

	recoveredManifest, events, err := CreateNewOrRecoverFromWithCompactionThreshold(manifestDirectoryPath, 0)
	assert.Nil(t, err)
	defer func() {
		_ = recoveredManifest.Close()
	}()
	assert.Equal(t, 1, len(events))

	snapshot, ok := events[0].(*StorageStateSnapshot)
	assert.True(t, ok)
	assert.Equal(t, []uint64{22, 23}, snapshot.MemtableIds)
	assert.Empty(t, snapshot.L0SSTableIds)
	assert.Equal(t, [][]uint64{nil, {30}}, snapshot.Levels)
	assert.Equal(t, uint64(30), snapshot.MaxId)
}

func TestCompactAnEmptyManifestWritesAnEmptySnapshot(t *testing.T) {
	manifestDirectoryPath := filepath.Join(".", "TestCompactAnEmptyManifestWritesAnEmptySnapshot")
	_ = os.RemoveAll(manifestDirectoryPath)
	assert.Nil(t, os.MkdirAll(manifestDirectoryPath, os.ModePerm))
	defer func() {
		_ = os.RemoveAll(manifestDirectoryPath)
	}()

	manifest, _, err := CreateNewOrRecoverFromWithCompactionThreshold(manifestDirectoryPath, 0)
	assert.Nil(t, err)
	assert.Nil(t, manifest.Compact())
	assert.Nil(t, manifest.Close())

	recoveredManifest, events, err := CreateNewOrRecoverFromWithCompactionThreshold(manifestDirectoryPath, 0)
	assert.Nil(t, err)
	defer func() {
		_ = recoveredManifest.Close()
	}()

	assert.Equal(t, 1, len(events))
	assert.Equal(t, SnapshotEventType, events[0].EventType())
}

func TestManifestIsAutomaticallyCompactedAfterTheThreshold(t *testing.T) {
	manifestDirectoryPath := filepath.Join(".", "TestManifestIsAutomaticallyCompactedAfterTheThreshold")
	_ = os.RemoveAll(manifestDirectoryPath)
	assert.Nil(t, os.MkdirAll(manifestDirectoryPath, os.ModePerm))

	manifest, _, err := CreateNewOrRecoverFromWithCompactionThreshold(manifestDirectoryPath, 3)
	assert.Nil(t, err)
	defer func() {
		_ = os.RemoveAll(manifestDirectoryPath)
	}()

	assert.Nil(t, manifest.Add(NewMemtableCreated(10)))
	assert.Nil(t, manifest.Add(NewMemtableCreated(11)))
	assert.Nil(t, manifest.Add(NewMemtableCreated(12)))
	assert.Nil(t, manifest.Close())

	recoveredManifest, events, err := CreateNewOrRecoverFromWithCompactionThreshold(manifestDirectoryPath, 3)
	assert.Nil(t, err)
	defer func() {
		_ = recoveredManifest.Close()
	}()
	assert.Equal(t, 1, len(events))

	snapshot, ok := events[0].(*StorageStateSnapshot)
	assert.True(t, ok)
	assert.Equal(t, []uint64{10, 11, 12}, snapshot.MemtableIds)
	assert.Equal(t, uint64(12), snapshot.MaxId)
}

func TestEventsAppendedAfterCompactionAreRecoveredAfterTheSnapshot(t *testing.T) {
	manifestDirectoryPath := filepath.Join(".", "TestEventsAppendedAfterCompactionAreRecoveredAfterTheSnapshot")
	_ = os.RemoveAll(manifestDirectoryPath)
	assert.Nil(t, os.MkdirAll(manifestDirectoryPath, os.ModePerm))
	defer func() {
		_ = os.RemoveAll(manifestDirectoryPath)
	}()

	manifest, _, err := CreateNewOrRecoverFromWithCompactionThreshold(manifestDirectoryPath, 0)
	assert.Nil(t, err)
	assert.Nil(t, manifest.Add(NewMemtableCreated(10)))
	assert.Nil(t, manifest.Compact())
	assert.Nil(t, manifest.Add(NewMemtableCreated(11)))
	assert.Nil(t, manifest.Close())

	recoveredManifest, events, err := CreateNewOrRecoverFromWithCompactionThreshold(manifestDirectoryPath, 0)
	assert.Nil(t, err)
	defer func() {
		_ = recoveredManifest.Close()
	}()

	assert.Equal(t, 2, len(events))
	assert.Equal(t, SnapshotEventType, events[0].EventType())
	assert.Equal(t, uint64(11), events[1].(*MemtableCreated).MemtableId)

	recoveredState := snapshotFromEvents(events)
	assert.Equal(t, []uint64{10, 11}, recoveredState.MemtableIds)
	assert.Equal(t, uint64(11), recoveredState.MaxId)
}

func TestRecoveryDropsAPartialEventAndKeepsFutureAppendsReadable(t *testing.T) {
	manifestDirectoryPath := filepath.Join(".", "TestRecoveryDropsAPartialEventAndKeepsFutureAppendsReadable")
	_ = os.RemoveAll(manifestDirectoryPath)
	assert.Nil(t, os.MkdirAll(manifestDirectoryPath, os.ModePerm))
	defer func() {
		_ = os.RemoveAll(manifestDirectoryPath)
	}()

	manifest, _, err := CreateNewOrRecoverFromWithCompactionThreshold(manifestDirectoryPath, 0)
	assert.Nil(t, err)
	assert.Nil(t, manifest.Add(NewMemtableCreated(10)))
	assert.Nil(t, manifest.Add(NewMemtableCreated(11)))

	partialEvent, err := os.OpenFile(filepath.Join(manifestDirectoryPath, "manifest"), os.O_WRONLY|os.O_APPEND, 0666)
	assert.Nil(t, err)
	_, err = partialEvent.Write([]byte{MemtableCreatedEventType, 1, 2})
	assert.Nil(t, err)
	assert.Nil(t, partialEvent.Close())
	assert.Nil(t, manifest.Close())

	recoveredManifest, events, err := CreateNewOrRecoverFromWithCompactionThreshold(manifestDirectoryPath, 0)
	assert.Nil(t, err)
	assert.Equal(t, 2, len(events))
	assert.Nil(t, recoveredManifest.Add(NewMemtableCreated(12)))
	assert.Nil(t, recoveredManifest.Close())

	recoveredManifest, events, err = CreateNewOrRecoverFromWithCompactionThreshold(manifestDirectoryPath, 0)
	assert.Nil(t, err)
	defer func() {
		_ = recoveredManifest.Close()
	}()
	assert.Equal(t, 3, len(events))
	assert.Equal(t, uint64(12), events[2].(*MemtableCreated).MemtableId)
}

func TestRecoveryIgnoresAStaleCompactionTemporaryFile(t *testing.T) {
	manifestDirectoryPath := filepath.Join(".", "TestRecoveryIgnoresAStaleCompactionTemporaryFile")
	_ = os.RemoveAll(manifestDirectoryPath)
	assert.Nil(t, os.MkdirAll(manifestDirectoryPath, os.ModePerm))
	defer func() {
		_ = os.RemoveAll(manifestDirectoryPath)
	}()

	manifest, _, err := CreateNewOrRecoverFromWithCompactionThreshold(manifestDirectoryPath, 0)
	assert.Nil(t, err)
	assert.Nil(t, manifest.Add(NewMemtableCreated(10)))
	tempPath := manifest.path + ".compaction.tmp"
	assert.Nil(t, os.WriteFile(tempPath, []byte("incomplete snapshot"), 0666))
	assert.Nil(t, manifest.Close())

	recoveredManifest, events, err := CreateNewOrRecoverFromWithCompactionThreshold(manifestDirectoryPath, 0)
	assert.Nil(t, err)
	defer func() {
		_ = recoveredManifest.Close()
	}()
	assert.Equal(t, 1, len(events))
	assert.Equal(t, uint64(10), events[0].(*MemtableCreated).MemtableId)
}

func TestConcurrentAppendsSurviveCompaction(t *testing.T) {
	manifestDirectoryPath := filepath.Join(".", "TestConcurrentAppendsSurviveCompaction")
	_ = os.RemoveAll(manifestDirectoryPath)
	assert.Nil(t, os.MkdirAll(manifestDirectoryPath, os.ModePerm))

	manifest, _, err := CreateNewOrRecoverFromWithCompactionThreshold(manifestDirectoryPath, 0)
	assert.Nil(t, err)
	defer func() {
		_ = os.RemoveAll(manifestDirectoryPath)
	}()
	assert.Nil(t, manifest.Add(NewMemtableCreated(1)))

	const numberOfEvents = 100
	var waitGroup sync.WaitGroup
	waitGroup.Add(numberOfEvents)
	for id := uint64(2); id <= numberOfEvents+1; id++ {
		go func(id uint64) {
			defer waitGroup.Done()
			assert.Nil(t, manifest.Add(NewMemtableCreated(id)))
		}(id)
	}
	assert.Nil(t, manifest.Compact())
	waitGroup.Wait()
	assert.Nil(t, manifest.Close())

	recoveredManifest, events, err := CreateNewOrRecoverFromWithCompactionThreshold(manifestDirectoryPath, 0)
	assert.Nil(t, err)
	defer func() {
		_ = recoveredManifest.Close()
	}()
	snapshot := snapshotFromEvents(events)
	assert.Equal(t, numberOfEvents+1, len(snapshot.MemtableIds))
	assert.Equal(t, uint64(numberOfEvents+1), snapshot.MaxId)
}
