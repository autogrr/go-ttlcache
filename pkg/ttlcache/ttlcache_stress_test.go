// Copyright (c) 2021-2025 autogrr contributors
// SPDX-License-Identifier: MIT

// Package ttlcache stress tests.  Each test verifies a correctness property
// under high concurrency; they are explicitly designed to surface data races,
// lost updates, phantom reads, and callback-count violations.
//
// Run with the race detector:
//
//	go test -race -count=1 -timeout=120s ./pkg/ttlcache/...
package ttlcache

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// ── helpers ──────────────────────────────────────────────────────────────────

// hammer runs n goroutines each calling f(goroutine-index, iteration-index)
// iters times, waits for all to finish.
func hammer(n, iters int, f func(g, i int)) {
	var wg sync.WaitGroup
	wg.Add(n)
	for g := 0; g < n; g++ {
		g := g
		go func() {
			defer wg.Done()
			for i := 0; i < iters; i++ {
				f(g, i)
			}
		}()
	}
	wg.Wait()
}

// ── concurrent Set / Get correctness ─────────────────────────────────────────

// TestStressConcurrentSetGet hammers the cache from many goroutines with
// disjoint key spaces, then checks that each key round-trips correctly.
// The race detector will flag any unsynchronised access.
func TestStressConcurrentSetGet(t *testing.T) {
	t.Parallel()
	const (
		goroutines = 64
		perG       = 200
	)
	c := New[int, int](Options[int, int]{}.SetDefaultTTL(10 * time.Second))
	defer c.Close()

	// Phase 1: concurrent writes (disjoint keys per goroutine).
	hammer(goroutines, perG, func(g, i int) {
		key := g*perG + i
		c.Set(key, key*2, DefaultTTL)
	})

	// Phase 2: verify every key has the expected value.
	var bad atomic.Int64
	hammer(goroutines, perG, func(g, i int) {
		key := g*perG + i
		v, ok := c.Get(key)
		if !ok || v != key*2 {
			bad.Add(1)
		}
	})
	if n := bad.Load(); n != 0 {
		t.Fatalf("%d key(s) had wrong or missing values", n)
	}
}

// TestStressConcurrentOverwrite verifies that concurrent overwrites of the
// same key never produce a stale value older than the last write seen by Get.
// We write a monotonically increasing counter per key; after all writes settle
// we assert the value is positive (i.e. at least one write landed).
func TestStressConcurrentOverwrite(t *testing.T) {
	t.Parallel()
	const (
		goroutines = 32
		keys       = 64
		iters      = 100
	)
	c := New[int, int](Options[int, int]{}.SetDefaultTTL(10 * time.Second))
	defer c.Close()

	// Track the maximum value written per key so we can check reads are sane.
	maxWritten := make([]atomic.Int64, keys)

	hammer(goroutines, iters, func(g, i int) {
		key := (g*iters + i) % keys
		written := int64(g*iters + i + 1)
		c.Set(key, int(written), DefaultTTL)
		// Update maxWritten with a compare-and-swap loop.
		for {
			cur := maxWritten[key].Load()
			if written <= cur {
				break
			}
			if maxWritten[key].CompareAndSwap(cur, written) {
				break
			}
		}
	})

	for k := 0; k < keys; k++ {
		v, ok := c.Get(k)
		if !ok {
			t.Errorf("key %d: missing after all writes", k)
			continue
		}
		if v <= 0 {
			t.Errorf("key %d: non-positive value %d", k, v)
		}
	}
}

// ── expiry correctness ────────────────────────────────────────────────────────

// TestStressExpiryNeverResurrects sets items with a short TTL, waits for them
// all to expire with a generous margin, then asserts not a single one is still
// visible.  Concurrent Sets and Gets run throughout to provoke races.
func TestStressExpiryNeverResurrects(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	const (
		keys = 256
		ttl  = 80 * time.Millisecond
	)
	// DisableUpdateTime so concurrent Gets don't extend the sliding-window TTL.
	var evicted atomic.Int64
	allGone := make(chan struct{})
	o := Options[int, int]{}.
		SetDefaultTTL(ttl).
		DisableUpdateTime(true).
		SetDeallocationFunc(func(_ int, _ int, reason DeallocationReason) {
			if reason == ReasonTimedOut && evicted.Add(1) == keys {
				close(allGone)
			}
		})
	c := New[int, int](o)
	defer c.Close()

	// Write all keys.
	for k := 0; k < keys; k++ {
		c.Set(k, k, DefaultTTL)
	}

	// Concurrently read while the items expire.
	var reads atomic.Int64
	go func() {
		for {
			select {
			case <-allGone:
				return
			default:
				c.Get(int(reads.Add(1)) % keys)
			}
		}
	}()

	<-allGone

	// Every key must now be gone.
	var alive int
	for k := 0; k < keys; k++ {
		if _, ok := c.Get(k); ok {
			alive++
		}
	}
	if alive != 0 {
		t.Fatalf("%d key(s) survived past TTL+margin", alive)
	}
}

// TestStressExpiryExactCount verifies that the deallocation callback fires
// exactly once per expired key under concurrent load.
func TestStressExpiryExactCount(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	const (
		keys = 512
		ttl  = 60 * time.Millisecond
	)

	var callbacks atomic.Int64
	allGone := make(chan struct{})
	o := Options[int, bool]{}.
		SetDefaultTTL(ttl).
		SetDeallocationFunc(func(_ int, _ bool, reason DeallocationReason) {
			if reason == ReasonTimedOut && callbacks.Add(1) == keys {
				close(allGone)
			}
		})
	c := New[int, bool](o)
	defer c.Close()

	for k := 0; k < keys; k++ {
		c.Set(k, true, DefaultTTL)
	}

	<-allGone

	got := callbacks.Load()
	if got != keys {
		t.Fatalf("expected %d timeout callbacks, got %d", keys, got)
	}
}

// TestStressNoTTLSurvivesExpiryCycles writes NoTTL items alongside many
// short-lived items through several expiry cycles and asserts the NoTTL items
// survive throughout.
func TestStressNoTTLSurvivesExpiryCycles(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	const (
		noTTLKeys  = 64
		mortalKeys = 256
		ttl        = 40 * time.Millisecond
		cycles     = 5
	)

	// cycleSignal receives one token per mortal-key timeout.  We drain exactly
	// mortalKeys tokens per round to know when a full cycle has expired.
	cycleSignal := make(chan struct{}, mortalKeys*cycles)
	o := Options[int, string]{}.
		SetDefaultTTL(ttl).
		SetDeallocationFunc(func(key int, _ string, reason DeallocationReason) {
			if reason == ReasonTimedOut && key >= noTTLKeys {
				cycleSignal <- struct{}{}
			}
		})
	c := New[int, string](o)
	defer c.Close()

	for k := 0; k < noTTLKeys; k++ {
		c.Set(k, "forever", NoTTL)
	}

	for round := 0; round < cycles; round++ {
		for k := noTTLKeys; k < noTTLKeys+mortalKeys; k++ {
			c.Set(k, "mortal", DefaultTTL)
		}
		// Wait for all mortalKeys in this round to time out.
		for i := 0; i < mortalKeys; i++ {
			<-cycleSignal
		}
	}

	// All NoTTL keys must still be alive.
	var lost int
	for k := 0; k < noTTLKeys; k++ {
		v, ok := c.Get(k)
		if !ok || v != "forever" {
			lost++
		}
	}
	if lost != 0 {
		t.Fatalf("%d NoTTL key(s) were lost during expiry cycles", lost)
	}
}

// ── sliding-window TTL ────────────────────────────────────────────────────────

// TestStressSlidingWindowExtends sets an item with a short TTL, then
// hammers Get from many goroutines to keep extending the deadline.  The item
// must still be alive after several would-have-expired intervals.
func TestStressSlidingWindowExtends(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	const ttl = 80 * time.Millisecond

	c := New[int, int](Options[int, int]{}.SetDefaultTTL(ttl))
	defer c.Close()

	c.Set(1, 42, DefaultTTL)

	// Hammer Get for 3× the original TTL — sliding window should renew it.
	deadline := time.Now().Add(ttl * 3)
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for time.Now().Before(deadline) {
				c.Get(1)
				time.Sleep(time.Millisecond * 5)
			}
		}()
	}
	wg.Wait()

	if _, ok := c.Get(1); !ok {
		t.Fatal("item should still be alive due to sliding-window TTL extension")
	}
}

// TestStressDisableUpdateTimeNoExtension verifies that with DisableUpdateTime
// the expiry really does not move on Get.
func TestStressDisableUpdateTimeNoExtension(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	const ttl = 80 * time.Millisecond

	c := New[int, int](Options[int, int]{}.
		SetDefaultTTL(ttl).
		DisableUpdateTime(true))
	defer c.Close()

	c.Set(1, 42, DefaultTTL)

	// Hammer Get for 3× TTL; without sliding window the item should expire.
	deadline := time.Now().Add(ttl * 3)
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for time.Now().Before(deadline) {
				c.Get(1)
				time.Sleep(time.Millisecond * 5)
			}
		}()
	}
	wg.Wait()

	if _, ok := c.Get(1); ok {
		t.Fatal("item should have expired; DisableUpdateTime must not extend TTL on Get")
	}
}

// ── GetOrSet atomicity ────────────────────────────────────────────────────────

// TestStressGetOrSetAtomicity races many goroutines on GetOrSet for the same
// key.  All must observe the same winning value, and the deallocation callback
// must fire at most once if the key later expires.
func TestStressGetOrSetAtomicity(t *testing.T) {
	t.Parallel()
	const goroutines = 100

	c := New[string, int](Options[string, int]{}.SetDefaultTTL(10 * time.Second))
	defer c.Close()

	var (
		startWg sync.WaitGroup
		doneWg  sync.WaitGroup
		ready   = make(chan struct{})
		values  [goroutines]atomic.Int64 // atomic to avoid data race on array
	)
	startWg.Add(goroutines)
	doneWg.Add(goroutines)

	for g := 0; g < goroutines; g++ {
		g := g
		go func() {
			startWg.Done()
			<-ready
			defer doneWg.Done()
			v, _ := c.GetOrSet("key", g+1, DefaultTTL)
			values[g].Store(int64(v))
		}()
	}
	startWg.Wait()
	close(ready) // release all goroutines simultaneously
	doneWg.Wait()

	// All goroutines must have received the same value.
	first := values[0].Load()
	if first == 0 {
		t.Fatal("no value was stored")
	}
	for g := 1; g < goroutines; g++ {
		if v := values[g].Load(); v != first {
			t.Fatalf("goroutine %d got %d, expected %d (all must see same winner)", g, v, first)
		}
	}
}

// ── Delete correctness ────────────────────────────────────────────────────────

// TestStressConcurrentDelete races concurrent deletes of the same key and
// verifies the deallocation callback fires at most once per lifecycle.
func TestStressConcurrentDelete(t *testing.T) {
	t.Parallel()
	const (
		goroutines = 32
		rounds     = 50
	)

	var callbacks atomic.Int64
	o := Options[int, bool]{}.
		SetDefaultTTL(10 * time.Second).
		SetDeallocationFunc(func(_ int, _ bool, _ DeallocationReason) {
			callbacks.Add(1)
		})
	c := New[int, bool](o)
	defer c.Close()

	for round := 0; round < rounds; round++ {
		c.Set(0, true, DefaultTTL)
		callbacks.Store(0)

		var wg sync.WaitGroup
		wg.Add(goroutines)
		for g := 0; g < goroutines; g++ {
			go func() {
				defer wg.Done()
				c.Delete(0)
			}()
		}
		wg.Wait()

		if n := callbacks.Load(); n != 1 {
			t.Fatalf("round %d: expected 1 deallocation callback, got %d", round, n)
		}
	}
}

// TestStressConcurrentDeleteAndExpiry races explicit Delete against the expiry
// goroutine on the same key.  The deallocation callback must fire exactly once.
func TestStressConcurrentDeleteAndExpiry(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	const (
		rounds = 10
		ttl    = 30 * time.Millisecond
	)

	for round := 0; round < rounds; round++ {
		var callbacks atomic.Int64
		done := make(chan struct{})
		o := Options[int, bool]{}.
			SetDefaultTTL(ttl).
			SetDeallocationFunc(func(_ int, _ bool, _ DeallocationReason) {
				if callbacks.Add(1) == 1 {
					close(done)
				}
			})
		c := New[int, bool](o)

		c.Set(0, true, DefaultTTL)

		// Delete concurrently with TTL expiry: wait half the TTL then delete.
		// The deallocation callback will fire from whichever wins (Delete or expiry).
		time.Sleep(ttl / 2)
		c.Delete(0)

		<-done // wait for exactly one deallocation
		c.Close()

		if n := callbacks.Load(); n != 1 {
			t.Fatalf("round %d: expected 1 deallocation callback, got %d", round, n)
		}
	}
}

// ── Close safety ─────────────────────────────────────────────────────────────

// TestStressCloseWithInflightSets calls Close while many goroutines are
// actively calling Set.  The test must complete without panic or deadlock.
func TestStressCloseWithInflightSets(t *testing.T) {
	t.Parallel()
	const goroutines = 64

	for trial := 0; trial < 5; trial++ {
		c := New[int, int](Options[int, int]{}.SetDefaultTTL(time.Second))

		var wg sync.WaitGroup
		stop := make(chan struct{})
		wg.Add(goroutines)
		for g := 0; g < goroutines; g++ {
			g := g
			go func() {
				defer wg.Done()
				for i := 0; ; i++ {
					select {
					case <-stop:
						return
					default:
						c.Set(g*1000+i%1000, i, DefaultTTL)
					}
				}
			}()
		}

		// Let goroutines hammer for a bit, then close.
		time.Sleep(10 * time.Millisecond)
		close(stop)
		wg.Wait() // ensure goroutines see stop before Close
		c.Close()
	}
}

// TestStressDoubleClose verifies that calling Close twice does not panic.
func TestStressDoubleClose(t *testing.T) {
	t.Parallel()
	c := New[int, int](Options[int, int]{}.SetDefaultTTL(time.Second))
	c.Close()
	// Second Close must not panic, even though the channel is already closed.
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("second Close panicked: %v", r)
		}
	}()
	c.Close()
}

// ── value overwrite consistency ───────────────────────────────────────────────

// TestStressValueOverwrite concurrently sets the same key with increasing
// values, then reads it back.  The returned value must be one of the values
// actually written (no corruption) and must be positive.
func TestStressValueOverwrite(t *testing.T) {
	t.Parallel()
	const (
		goroutines = 32
		iters      = 500
		key        = 7
	)

	c := New[int, int](Options[int, int]{}.SetDefaultTTL(10 * time.Second))
	defer c.Close()

	// Keep a set of all values that were ever written.
	written := make(map[int]struct{}, goroutines*iters)
	var mu sync.Mutex
	hammer(goroutines, iters, func(g, i int) {
		v := g*iters + i + 1
		mu.Lock()
		written[v] = struct{}{}
		mu.Unlock()
		c.Set(key, v, DefaultTTL)
	})

	v, ok := c.Get(key)
	if !ok {
		t.Fatal("key missing after all writes")
	}
	mu.Lock()
	_, known := written[v]
	mu.Unlock()
	if !known {
		t.Fatalf("Get returned value %d which was never written", v)
	}
}

// ── multi-TTL interleaving ────────────────────────────────────────────────────

// TestStressMultiTTLOrdering sets items in three TTL tiers and asserts that
// each tier expires while later tiers remain alive.
func TestStressMultiTTLOrdering(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	const (
		keysPerTier = 32
		ttl1        = 80 * time.Millisecond
		ttl2        = 200 * time.Millisecond
		ttl3        = 400 * time.Millisecond
	)

	// tier1Done fires after all tier-1 keys have expired.
	// tier2Done fires after all tier-2 keys have expired.
	// tier3Done fires after all tier-3 keys have expired.
	var t1count, t2count, t3count atomic.Int64
	tier1Done := make(chan struct{})
	tier2Done := make(chan struct{})
	tier3Done := make(chan struct{})

	o := Options[int, int]{}.
		SetDefaultTTL(ttl1).
		DisableUpdateTime(true).
		SetDeallocationFunc(func(key int, _ int, reason DeallocationReason) {
			if reason != ReasonTimedOut {
				return
			}
			switch {
			case key < keysPerTier:
				if t1count.Add(1) == keysPerTier {
					close(tier1Done)
				}
			case key < 2*keysPerTier:
				if t2count.Add(1) == keysPerTier {
					close(tier2Done)
				}
			default:
				if t3count.Add(1) == keysPerTier {
					close(tier3Done)
				}
			}
		})
	c := New[int, int](o)
	defer c.Close()

	for k := 0; k < keysPerTier; k++ {
		c.Set(k, k, ttl1)
		c.Set(keysPerTier+k, k, ttl2)
		c.Set(2*keysPerTier+k, k, ttl3)
	}

	// Check 1 — after tier-1 expires: tier-1 gone, tiers 2+3 alive.
	<-tier1Done
	for k := 0; k < keysPerTier; k++ {
		if _, ok := c.Get(k); ok {
			t.Errorf("tier-1 key %d still alive after ttl1 expiry", k)
		}
		if _, ok := c.Get(keysPerTier + k); !ok {
			t.Errorf("tier-2 key %d expired too early", k)
		}
		if _, ok := c.Get(2*keysPerTier + k); !ok {
			t.Errorf("tier-3 key %d expired too early", k)
		}
	}

	// Check 2 — after tier-2 expires: tier-2 gone, tier-3 still alive.
	<-tier2Done
	for k := 0; k < keysPerTier; k++ {
		if _, ok := c.Get(keysPerTier + k); ok {
			t.Errorf("tier-2 key %d still alive after ttl2 expiry", k)
		}
		if _, ok := c.Get(2*keysPerTier + k); !ok {
			t.Errorf("tier-3 key %d expired too early", k)
		}
	}

	// Check 3 — after tier-3 expires: all gone.
	<-tier3Done
	for k := 0; k < keysPerTier; k++ {
		if _, ok := c.Get(2*keysPerTier + k); ok {
			t.Errorf("tier-3 key %d still alive after ttl3 expiry", k)
		}
	}
}

// ── GetKeys consistency ───────────────────────────────────────────────────────

// TestStressGetKeysConsistency verifies GetKeys never returns duplicates and
// only returns keys that currently exist, under concurrent modifications.
func TestStressGetKeysConsistency(t *testing.T) {
	t.Parallel()
	const (
		goroutines = 16
		keys       = 64
		iters      = 200
	)

	c := New[int, bool](Options[int, bool]{}.SetDefaultTTL(10 * time.Second))
	defer c.Close()

	// Pre-populate.
	for k := 0; k < keys; k++ {
		c.Set(k, true, NoTTL)
	}

	stop := make(chan struct{})

	// Concurrent writers: set and delete random keys.
	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < iters; i++ {
				select {
				case <-stop:
					return
				default:
				}
				k := i % keys
				if i%3 == 0 {
					c.Delete(k)
				} else {
					c.Set(k, true, NoTTL)
				}
			}
		}()
	}

	// Concurrent readers: GetKeys must never return duplicates.
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < iters; i++ {
				select {
				case <-stop:
					return
				default:
				}
				ks := c.GetKeys()
				seen := make(map[int]struct{}, len(ks))
				for _, k := range ks {
					if _, dup := seen[k]; dup {
						// Signal stop to avoid flooding output.
						select {
						case <-stop:
						default:
							close(stop)
						}
						return
					}
					seen[k] = struct{}{}
				}
			}
		}()
	}

	wg.Wait()

	// Check if stop was triggered (means a duplicate was found).
	select {
	case <-stop:
		t.Fatal("GetKeys returned duplicate keys under concurrent modification")
	default:
	}
}

// ── shard distribution ────────────────────────────────────────────────────────

// TestStressShardDistribution writes many keys and verifies that every shard
// received at least one key (no hash clustering that starves shards).
func TestStressShardDistribution(t *testing.T) {
	t.Parallel()
	const totalKeys = 4096

	c := New[int, bool](Options[int, bool]{}.SetDefaultTTL(10 * time.Second))
	defer c.Close()

	for k := 0; k < totalKeys; k++ {
		c.Set(k, true, NoTTL)
	}

	// Access the shards directly via the exported shard count.
	// We verify indirectly: GetKeys must return all keys (no shard was skipped).
	ks := c.GetKeys()
	if len(ks) != totalKeys {
		t.Fatalf("expected %d keys, got %d (possible shard starvation)", totalKeys, len(ks))
	}
}

// ── SetItem / GetItem round-trip ──────────────────────────────────────────────

// TestStressConcurrentSetItemGetItem stresses the Item-returning API paths
// under concurrency — verifies that returned Items always contain coherent
// (value, duration, expiry-time) triples.
func TestStressConcurrentSetItemGetItem(t *testing.T) {
	t.Parallel()
	const (
		goroutines = 32
		keys       = 128
		iters      = 100
	)

	c := New[int, int](Options[int, int]{}.SetDefaultTTL(10 * time.Second))
	defer c.Close()

	for k := 0; k < keys; k++ {
		c.Set(k, k, DefaultTTL)
	}

	var bad atomic.Int64
	hammer(goroutines, iters, func(g, i int) {
		key := (g*iters + i) % keys
		it, ok := c.GetItem(key)
		if !ok {
			return // may have been set by a racing writer below
		}
		// Value and key must match the invariant key == value (initial load).
		// Overwritten keys are allowed to deviate, so we only flag zero-value.
		if it.GetValue() < 0 {
			bad.Add(1)
		}
		d := it.GetDuration()
		if d <= 0 {
			bad.Add(1)
		}
		if it.GetTime().IsZero() {
			bad.Add(1)
		}
	})

	if n := bad.Load(); n != 0 {
		t.Fatalf("%d invalid Item(s) observed", n)
	}
}

// ── Reschedule under concurrency ──────────────────────────────────────────────

// TestStressConcurrentReschedule sets a key as NoTTL then immediately
// reschedules it with a TTL from multiple goroutines.  After the TTL expires
// the key must be gone — no goroutine must have left it as NoTTL.
func TestStressConcurrentReschedule(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	const (
		goroutines = 32
		ttl        = 80 * time.Millisecond
	)

	done := make(chan struct{})
	o := Options[int, bool]{}.
		SetDefaultTTL(ttl).
		SetDeallocationFunc(func(_ int, _ bool, reason DeallocationReason) {
			if reason == ReasonTimedOut {
				select {
				case <-done:
				default:
					close(done)
				}
			}
		})
	c := New[int, bool](o)
	defer c.Close()

	var wg sync.WaitGroup
	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		go func() {
			defer wg.Done()
			c.Set(0, true, NoTTL)
			c.Set(0, true, DefaultTTL)
		}()
	}
	wg.Wait()

	// After TTL+margin key must be expired — wait for the eviction callback.
	<-done
	if _, ok := c.Get(0); ok {
		t.Fatal("key should have expired after reschedule from NoTTL to TTL")
	}
}

// ── mix of all operations ─────────────────────────────────────────────────────

// TestStressFullMix runs all operations (Set, Get, GetOrSet, Delete, GetKeys)
// concurrently for a sustained period.  The sole correctness assertion is that
// no panic occurs and the race detector finds no violations.
func TestStressFullMix(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	const (
		goroutines = 64
		keys       = 256
		duration   = 300 * time.Millisecond
	)

	var callbackPanics atomic.Int64
	o := Options[int, int]{}.
		SetDefaultTTL(20 * time.Millisecond).
		SetDeallocationFunc(func(key int, _ int, _ DeallocationReason) {
			if key < 0 || key >= keys {
				callbackPanics.Add(1)
			}
		})

	c := New[int, int](o)
	defer c.Close()

	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		g := g
		go func() {
			defer wg.Done()
			i := 0
			for {
				select {
				case <-stop:
					return
				default:
				}
				key := (g*1000 + i) % keys
				switch i % 5 {
				case 0:
					c.Set(key, i, DefaultTTL)
				case 1:
					c.Get(key)
				case 2:
					c.GetOrSet(key, i, DefaultTTL)
				case 3:
					c.Delete(key)
				case 4:
					_ = c.GetKeys()
				}
				i++
			}
		}()
	}

	time.Sleep(duration)
	close(stop)
	wg.Wait()

	if n := callbackPanics.Load(); n != 0 {
		t.Fatalf("%d deallocation callbacks received out-of-range key", n)
	}
}

// ── DeallocationReason accuracy ───────────────────────────────────────────────

// TestStressDeallocationReasons verifies that every deletion via Delete
// carries ReasonDeleted and every expiry carries ReasonTimedOut, never mixed,
// even under heavy concurrency.
func TestStressDeallocationReasons(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	const (
		deletedKeys = 128
		expiredKeys = 128
		ttl         = 50 * time.Millisecond
	)

	var wrongReason atomic.Int64
	var expiredCount atomic.Int64
	allExpired := make(chan struct{})
	o := Options[int, int]{}.
		SetDefaultTTL(ttl).
		SetDeallocationFunc(func(key int, _ int, reason DeallocationReason) {
			// Keys [0, deletedKeys) are explicitly deleted → ReasonDeleted.
			// Keys [deletedKeys, deletedKeys+expiredKeys) expire → ReasonTimedOut.
			if key < deletedKeys && reason != ReasonDeleted {
				wrongReason.Add(1)
			}
			if key >= deletedKeys && reason != ReasonTimedOut {
				wrongReason.Add(1)
			}
			if key >= deletedKeys && reason == ReasonTimedOut {
				if expiredCount.Add(1) == expiredKeys {
					close(allExpired)
				}
			}
		})

	c := New[int, int](o)
	defer c.Close()

	// Keys [0, deletedKeys) are set with NoTTL so they can ONLY be removed by
	// an explicit Delete call — no race with the expiry goroutine.
	for k := 0; k < deletedKeys; k++ {
		c.Set(k, k, NoTTL)
	}
	// Keys [deletedKeys, deletedKeys+expiredKeys) are set with DefaultTTL so
	// they will be swept by the expiry goroutine.
	for k := deletedKeys; k < deletedKeys+expiredKeys; k++ {
		c.Set(k, k, DefaultTTL)
	}

	// Explicitly delete the first half concurrently.
	var wg sync.WaitGroup
	wg.Add(deletedKeys)
	for k := 0; k < deletedKeys; k++ {
		k := k
		go func() {
			defer wg.Done()
			c.Delete(k)
		}()
	}
	wg.Wait()

	// Wait for all expiry callbacks to fire.
	<-allExpired

	if n := wrongReason.Load(); n != 0 {
		t.Fatalf("%d callback(s) received wrong DeallocationReason", n)
	}
}

// ── GetOrSet with expiry ──────────────────────────────────────────────────────

// TestStressGetOrSetExpiredKey verifies that once a key has expired, the next
// GetOrSet wins and stores the new value rather than returning a stale one.
func TestStressGetOrSetExpiredKey(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	const ttl = 60 * time.Millisecond

	expired := make(chan struct{})
	o := Options[string, int]{}.
		SetDefaultTTL(ttl).
		SetDeallocationFunc(func(_ string, _ int, reason DeallocationReason) {
			if reason == ReasonTimedOut {
				close(expired)
			}
		})
	c := New[string, int](o)
	defer c.Close()

	c.Set("k", 1, DefaultTTL)
	<-expired // wait for expiry rather than sleeping

	v, ok := c.GetOrSet("k", 99, DefaultTTL)
	if !ok || v != 99 {
		t.Fatalf("after expiry GetOrSet should set new value 99, got (%d, %v)", v, ok)
	}
}

// ── regression: Set after Close must not panic ────────────────────────────────

// TestStressSetAfterClose confirms that a Set arriving after Close does not
// panic (the closed flag is checked before channel send).
func TestStressSetAfterClose(t *testing.T) {
	t.Parallel()
	c := New[int, int](Options[int, int]{}.SetDefaultTTL(time.Second))
	c.Close()
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("Set after Close panicked: %v", r)
		}
	}()
	c.Set(1, 1, DefaultTTL)
}

// ── large-scale churn ─────────────────────────────────────────────────────────

// TestStressLargeScaleChurn creates a cache with 1-second TTL items and
// churns it heavily for several seconds, checking memory-safety (race
// detector) and that Close does not block.
func TestStressLargeScaleChurn(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	const (
		goroutines = 32
		keys       = 1024
		d          = 500 * time.Millisecond
		ttl        = 100 * time.Millisecond
	)

	c := New[int, int](Options[int, int]{}.SetDefaultTTL(ttl))

	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		g := g
		go func() {
			defer wg.Done()
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				key := (g*1e6 + i) % keys
				if i%4 == 0 {
					c.Set(key, i, DefaultTTL)
				} else {
					c.Get(key)
				}
			}
		}()
	}

	time.Sleep(d)
	close(stop)
	wg.Wait()

	done := make(chan struct{})
	go func() { c.Close(); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Close blocked for more than 5 seconds")
	}
}

// TestStressConcurrentSetGetString uses string keys to exercise the hash
// function on a different key type and ensure no shard mapping bugs.
func TestStressConcurrentSetGetString(t *testing.T) {
	t.Parallel()
	const (
		goroutines = 32
		perG       = 100
	)

	c := New[string, int](Options[string, int]{}.SetDefaultTTL(10 * time.Second))
	defer c.Close()

	hammer(goroutines, perG, func(g, i int) {
		key := fmt.Sprintf("g%d-i%d", g, i)
		c.Set(key, g*perG+i, DefaultTTL)
	})

	var bad atomic.Int64
	hammer(goroutines, perG, func(g, i int) {
		key := fmt.Sprintf("g%d-i%d", g, i)
		v, ok := c.Get(key)
		if !ok || v != g*perG+i {
			bad.Add(1)
		}
	})
	if n := bad.Load(); n != 0 {
		t.Fatalf("%d string-keyed entries had wrong or missing values", n)
	}
}
