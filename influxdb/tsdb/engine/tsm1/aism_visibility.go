package tsm1

import (
	"sync"
	"sync/atomic"
)

// aismVisibility detects overlapping publication and deletion without making
// queries or ordinary writes acquire a shared publication lock. A counter is
// required because native tombstone commits can run concurrently.
type aismVisibility struct {
	active   atomic.Int64
	revision atomic.Uint64
}

type aismMutation struct {
	view *aismVisibility
	once sync.Once
}

func (v *aismVisibility) begin() *aismMutation {
	if v == nil {
		return nil
	}
	v.active.Add(1)
	v.revision.Add(1)
	return &aismMutation{view: v}
}

func (m *aismMutation) finish() {
	if m == nil {
		return
	}
	m.once.Do(func() {
		m.view.revision.Add(1)
		m.view.active.Add(-1)
	})
}

func (v *aismVisibility) startRead() (uint64, bool) {
	revision := v.revision.Load()
	return revision, v.active.Load() == 0
}

func (v *aismVisibility) valid(revision uint64) bool {
	return v.active.Load() == 0 && v.revision.Load() == revision
}
