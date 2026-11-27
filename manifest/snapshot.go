package manifest

// snapshotFromEvents replays the events in the manifest and returns the complete
// state they represent.
func snapshotFromEvents(events []Event) *StorageStateSnapshot {
	state := &StorageStateSnapshot{}
	memtableIds := make([]uint64, 0)
	memtableIdSet := make(map[uint64]struct{})

	for _, event := range events {
		switch typedEvent := event.(type) {
		case *MemtableCreated:
			if _, exists := memtableIdSet[typedEvent.MemtableId]; !exists {
				memtableIdSet[typedEvent.MemtableId] = struct{}{}
				memtableIds = append(memtableIds, typedEvent.MemtableId)
			}
			updateMaxId(state, typedEvent.MemtableId)
		case *SSTableFlushed:
			delete(memtableIdSet, typedEvent.SsTableId)
			memtableIds = removeId(memtableIds, typedEvent.SsTableId)
			state.L0SSTableIds = append(state.L0SSTableIds, typedEvent.SsTableId)
			updateMaxId(state, typedEvent.SsTableId)
		case *CompactionDone:
			applyCompactionDoneToSnapshot(state, typedEvent)
		case *StorageStateSnapshot:
			state = cloneSnapshot(typedEvent)
			memtableIds = append(memtableIds[:0], state.MemtableIds...)
			memtableIdSet = make(map[uint64]struct{}, len(memtableIds))
			for _, id := range memtableIds {
				memtableIdSet[id] = struct{}{}
			}
		}
	}

	state.MemtableIds = memtableIds
	return state
}

func applyCompactionDoneToSnapshot(state *StorageStateSnapshot, compactionDone *CompactionDone) {
	description := compactionDone.Description
	if description.UpperLevel == -1 {
		state.L0SSTableIds = removeIds(state.L0SSTableIds, description.UpperLevelSSTableIds)
	} else if description.UpperLevel > 0 {
		ensureLevelExists(state, description.UpperLevel)
		state.Levels[description.UpperLevel-1] = nil
	}

	if description.LowerLevel > 0 {
		ensureLevelExists(state, description.LowerLevel)
		lowerLevel := state.Levels[description.LowerLevel-1]
		lowerLevel = removeIds(lowerLevel, description.LowerLevelSSTableIds)
		state.Levels[description.LowerLevel-1] = append(lowerLevel, compactionDone.NewSSTableIds...)
	}

	for _, id := range compactionDone.NewSSTableIds {
		updateMaxId(state, id)
	}
	for _, id := range description.UpperLevelSSTableIds {
		updateMaxId(state, id)
	}
	for _, id := range description.LowerLevelSSTableIds {
		updateMaxId(state, id)
	}
}

func cloneSnapshot(snapshot *StorageStateSnapshot) *StorageStateSnapshot {
	cloned := &StorageStateSnapshot{
		MemtableIds:  append([]uint64(nil), snapshot.MemtableIds...),
		L0SSTableIds: append([]uint64(nil), snapshot.L0SSTableIds...),
		Levels:       make([][]uint64, len(snapshot.Levels)),
		MaxId:        snapshot.MaxId,
	}
	for level, ids := range snapshot.Levels {
		cloned.Levels[level] = append([]uint64(nil), ids...)
	}
	return cloned
}

func ensureLevelExists(snapshot *StorageStateSnapshot, level int) {
	for len(snapshot.Levels) < level {
		snapshot.Levels = append(snapshot.Levels, nil)
	}
}

func removeIds(ids []uint64, idsToRemove []uint64) []uint64 {
	if len(idsToRemove) == 0 {
		return ids
	}
	removeSet := make(map[uint64]struct{}, len(idsToRemove))
	for _, id := range idsToRemove {
		removeSet[id] = struct{}{}
	}
	remaining := ids[:0]
	for _, id := range ids {
		if _, remove := removeSet[id]; !remove {
			remaining = append(remaining, id)
		}
	}
	return remaining
}

func removeId(ids []uint64, idToRemove uint64) []uint64 {
	for index, id := range ids {
		if id == idToRemove {
			return append(ids[:index], ids[index+1:]...)
		}
	}
	return ids
}

func updateMaxId(snapshot *StorageStateSnapshot, id uint64) {
	if id > snapshot.MaxId {
		snapshot.MaxId = id
	}
}
