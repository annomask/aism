package tsm1

import (
	"context"
	"sync"
	"testing"

	"github.com/influxdata/influxdb/v2/influxql/query"
	"github.com/influxdata/influxdb/v2/tsdb"
	"github.com/influxdata/influxdb/v2/tsdb/engine/tsm1/aism"
	"github.com/stretchr/testify/require"
)

func TestAISMGroupViewRetriesAnyShardMutation(t *testing.T) {
	key := []byte("cpu,host=h#!~#i")
	engines := []*Engine{
		{id: 1, Cache: NewCache(1<<20, tsdb.EngineTags{}), FileStore: NewFileStore(t.TempDir(), tsdb.EngineTags{})},
		{id: 2, Cache: NewCache(1<<20, tsdb.EngineTags{}), FileStore: NewFileStore(t.TempDir(), tsdb.EngineTags{})},
	}
	require.NoError(t, engines[0].Cache.Write(key, Values{NewIntegerValue(1, 1)}))
	require.NoError(t, engines[1].Cache.Write(key, Values{NewIntegerValue(2, 2)}))
	var once sync.Once
	version := &aismBucketVersion{generations: map[*FileStore]uint64{engines[0].FileStore: 0, engines[1].FileStore: 0}}
	v, ok := captureAISMGroupAfterCapture(engines, key, 0, 10, version, func() {
		once.Do(func() {
			change := engines[0].FileStore.aismVisibility.begin()
			require.NoError(t, engines[0].Cache.Write(key, Values{NewIntegerValue(1, 99)}))
			change.finish()
		})
	})
	require.True(t, ok)
	defer v.Close()
	require.Equal(t, Values{NewIntegerValue(1, 99), NewIntegerValue(2, 2)}, v.cached)
	change := engines[1].FileStore.aismVisibility.begin()
	defer change.finish()
	v, ok = captureAISMGroup(engines, key, 0, 10, version)
	require.False(t, ok)
	require.Nil(t, v)
}

func TestAISMGroupViewRejectsContinuousMutation(t *testing.T) {
	e := &Engine{Cache: NewCache(1<<20, tsdb.EngineTags{}), FileStore: NewFileStore(t.TempDir(), tsdb.EngineTags{})}
	version := &aismBucketVersion{generations: map[*FileStore]uint64{e.FileStore: 0}}
	v, ok := captureAISMGroupAfterCapture([]*Engine{e}, []byte("k"), 0, 10, version, func() {
		e.FileStore.aismVisibility.begin().finish()
	})
	require.False(t, ok)
	require.Nil(t, v)
}

func TestAISMGroupViewRejectsClosedMemberWithResidualCache(t *testing.T) {
	e := &Engine{Cache: NewCache(1<<20, tsdb.EngineTags{}), FileStore: NewFileStore(t.TempDir(), tsdb.EngineTags{})}
	key := []byte("cpu,host=h#!~#i")
	require.NoError(t, e.Cache.Write(key, Values{NewIntegerValue(1, 99)}))
	version := &aismBucketVersion{generations: map[*FileStore]uint64{e.FileStore: 0}}
	var once sync.Once
	v, ok := captureAISMGroupAfterCapture([]*Engine{e}, key, 0, 10, version, func() {
		once.Do(func() { require.NoError(t, e.FileStore.Close()) })
	})
	if v != nil {
		defer v.Close()
	}
	require.False(t, ok, "closed member was accepted as a cache-only source")
	require.Nil(t, v)
	// A caller retaining the old engine after Close must also be rejected.
	v, ok = captureAISMGroup([]*Engine{e}, key, 0, 10, version)
	if v != nil {
		defer v.Close()
	}
	require.False(t, ok)
	require.Nil(t, v)
}

func TestAISMGroupCancelledBeforeCatalogAccess(t *testing.T) {
	ctx, _ := aism.WithTrace(context.Background(), "cross")
	ctx, cancel := context.WithCancel(ctx)
	cancel()
	e := &Engine{}
	itr, handled, err := e.CreateGroupIterator(ctx, "cpu", query.IteratorOptions{}, []tsdb.Engine{e, e})
	require.Nil(t, itr)
	require.True(t, handled)
	require.ErrorIs(t, err, context.Canceled)
}
