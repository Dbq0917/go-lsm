package state

import (
	"go-lsm/compact/meta"
	"go-lsm/kv"
	"go-lsm/manifest"
	"go-lsm/table"
	"go-lsm/table/block"
	"go-lsm/test_utility"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestStorageStateRecoversFromACompactedManifestSnapshot(t *testing.T) {
	rootPath := test_utility.SetupADirectoryWithTestName(t)
	defer func() {
		test_utility.CleanupDirectoryWithTestName(t)
	}()

	ssTableBuilder := table.NewSSTableBuilder(block.DefaultBlockSize)
	ssTableBuilder.Add(kv.NewStringKeyWithTimestamp("consensus", 10), kv.NewStringValue("paxos"))
	newSSTable, err := ssTableBuilder.Build(2, rootPath)
	assert.Nil(t, err)

	manifestRecorder, _, err := manifest.CreateNewOrRecoverFromWithCompactionThreshold(rootPath, 0)
	assert.Nil(t, err)
	assert.Nil(t, manifestRecorder.Add(manifest.NewMemtableCreated(1)))
	assert.Nil(t, manifestRecorder.Add(manifest.NewSSTableFlushed(1)))
	assert.Nil(t, manifestRecorder.Add(manifest.NewCompactionDone(
		[]uint64{newSSTable.Id()},
		meta.SimpleLeveledCompactionDescription{
			UpperLevel:           -1,
			LowerLevel:           1,
			UpperLevelSSTableIds: []uint64{1},
		},
	)))
	assert.Nil(t, manifestRecorder.Compact())
	assert.Nil(t, manifestRecorder.Close())

	storageState, err := NewStorageStateWithOptions(StorageOptions{
		MemTableSizeInBytes:   1 << 20,
		SSTableSizeInBytes:    1 << 20,
		Path:                  rootPath,
		MaximumMemtables:      10,
		FlushMemtableDuration: time.Minute,
		CompactionOptions: CompactionOptions{
			StrategyOptions: SimpleLeveledCompactionOptions{
				NumberOfSSTablesRatioPercentage: 200,
				MaxLevels:                       totalLevels,
				Level0FilesCompactionTrigger:    6,
			},
		},
	})
	assert.Nil(t, err)
	defer storageState.Close()

	assert.Equal(t, 1, storageState.TotalSSTablesAtLevel(1))
	value, ok := storageState.Get(kv.NewStringKeyWithTimestamp("consensus", 11))
	assert.True(t, ok)
	assert.Equal(t, kv.NewStringValue("paxos"), value)
}
