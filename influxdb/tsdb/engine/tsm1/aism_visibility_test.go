package tsm1

import (
	"sync"
	"testing"
)

func TestAISMVisibilityParallelMutations(t *testing.T) {
	var visibility aismVisibility
	revision, stable := visibility.startRead()
	if !stable || !visibility.valid(revision) {
		t.Fatal("zero-value visibility is not stable")
	}
	first, second := visibility.begin(), visibility.begin()
	if _, stable := visibility.startRead(); stable {
		t.Fatal("two simultaneous changes looked stable")
	}
	first.finish()
	if visibility.valid(revision) {
		t.Fatal("finishing one of two changes exposed an incomplete view")
	}
	if _, stable := visibility.startRead(); stable {
		t.Fatal("one outstanding change looked stable")
	}
	second.finish()
	if visibility.valid(revision) {
		t.Fatal("accepted a view spanning a completed change")
	}
	current, stable := visibility.startRead()
	if !stable || !visibility.valid(current) || visibility.active.Load() != 0 {
		t.Fatal("completed changes did not publish a stable view")
	}
	first.finish()
	second.finish()
	if !visibility.valid(current) || visibility.active.Load() != 0 {
		t.Fatal("repeated cleanup changed visibility")
	}
}

func TestAISMVisibilityConcurrentCompletion(t *testing.T) {
	var visibility aismVisibility
	const writers = 8
	var ready, finished sync.WaitGroup
	ready.Add(writers)
	finished.Add(writers)
	release := make(chan struct{})
	for i := 0; i < writers; i++ {
		go func() {
			defer finished.Done()
			mutation := visibility.begin()
			ready.Done()
			<-release
			mutation.finish()
		}()
	}
	ready.Wait()
	if _, stable := visibility.startRead(); stable {
		t.Fatal("parallel delete batches appeared stable")
	}
	if visibility.active.Load() != writers {
		t.Fatal("lost an active mutation")
	}
	close(release)
	finished.Wait()
	revision, stable := visibility.startRead()
	if !stable || !visibility.valid(revision) || visibility.active.Load() != 0 || revision != writers*2 {
		t.Fatal("incorrect concurrent completion state")
	}
}
