// Copyright (c) 2021-2025 autogrr contributors
// SPDX-License-Identifier: MIT

package ttlcache

import (
	"hash/maphash"
	"time"

	"github.com/autogrr/go-ttlcache/pkg/timecache"
)

// New creates a Cache with the given options and starts the expiry goroutine.
func New[K comparable, V any](options Options[K, V]) *Cache[K, V] {
	n := options.shards
	if n <= 0 {
		n = defaultShards
	}
	n = nextPow2(n) // enforce power-of-2 for the bitmask in shard()

	shards := make([]cacheMap[K, V], n)
	for i := range shards {
		shards[i].m = make(map[K]Item[V])
		shards[i].idx = i
	}

	c := Cache[K, V]{
		shards: shards,
		seed:   maphash.MakeSeed(),
		o:      options,
		ch:     make(chan shardWakeup, max(1024, n*64)),
		done:   make(chan struct{}),
	}

	switch {
	case options.defaultTTL != NoTTL && options.defaultResolution == 0:
		c.tc = timecache.New(timecache.Options{}.Round(options.defaultTTL / 2))
	case options.defaultResolution != 0:
		c.tc = timecache.New(timecache.Options{}.Round(options.defaultResolution))
	default:
		// No TTL configured — allocate a timecache with default (1 s) resolution so
		// that explicit per-item durations still use the cached clock.
		c.tc = timecache.New(timecache.Options{})
	}

	go c.startExpirations()
	return &c
}

// Get returns the value for key, or the zero value and false if not found.
func (c *Cache[K, V]) Get(key K) (V, bool) {
	it, ok := c.getItem(key)
	if !ok {
		return *new(V), false
	}
	return it.v, true
}

// GetItem returns the full Item for key.
func (c *Cache[K, V]) GetItem(key K) (Item[V], bool) {
	return c.getItem(key)
}

// getItem is the single authoritative implementation used by Get and GetItem.
//
// Sliding-window TTL correctness: when noUpdateTime is false we must hold the
// write lock for the entire read+conditional-update to avoid a TOCTOU race
// where the expiry goroutine deletes the key between our read (RLock) and the
// subsequent write (Lock), which would otherwise resurrect an expired entry.
//
// When noUpdateTime is true (no sliding window), a plain read lock suffices.
func (c *Cache[K, V]) getItem(key K) (Item[V], bool) {
	cm := c.shard(key)
	if c.o.noUpdateTime {
		return cm.load(key) // read lock only — no sliding window
	}

	// Sliding window: hold write lock for atomic read+bump.
	cm.Lock()
	defer cm.Unlock()
	it, ok := cm.m[key]
	if !ok || it.t.IsZero() {
		return it, ok
	}
	if _, newExpiry := c.getDuration(it.d); newExpiry.After(it.t) {
		it = c._s(cm, key, it)
	}
	return it, ok
}

// GetOrSet atomically returns the existing value for key, or stores value and
// returns it if the key is absent.  Returns (value, true) on success.
func (c *Cache[K, V]) GetOrSet(key K, value V, duration time.Duration) (V, bool) {
	it, ok := c.getOrSet(key, Item[V]{v: value, d: c.fixupDuration(duration)})
	if !ok {
		return *new(V), false
	}
	return it.v, true
}

// GetOrSetItem is like GetOrSet but returns the full Item.
func (c *Cache[K, V]) GetOrSetItem(key K, value V, duration time.Duration) (Item[V], bool) {
	it, ok := c.getOrSet(key, Item[V]{v: value, d: c.fixupDuration(duration)})
	if !ok {
		return Item[V]{}, false
	}
	return it, true
}

// Set stores value for key with the given duration.  Always returns true.
func (c *Cache[K, V]) Set(key K, value V, duration time.Duration) bool {
	c.set(key, Item[V]{v: value, d: c.fixupDuration(duration)})
	return true
}

// SetItem stores value for key and returns the stored Item.
func (c *Cache[K, V]) SetItem(key K, value V, duration time.Duration) Item[V] {
	return c.set(key, Item[V]{v: value, d: c.fixupDuration(duration)})
}

// Delete removes key from the cache and fires the deallocation callback if set.
func (c *Cache[K, V]) Delete(key K) {
	c.delete(key, ReasonDeleted)
}

// GetKeys returns a snapshot of all keys currently in the cache.
func (c *Cache[K, V]) GetKeys() []K {
	return c.getkeys()
}

// Close shuts down the expiry goroutine.  The cache must not be used afterwards.
// Concurrent Set calls in flight during Close are safe: notify() checks the
// closed flag before touching the channel.
// Close stops the expiry goroutine and releases resources.  It is safe to
// call Close more than once; only the first call has any effect.
func (c *Cache[K, V]) Close() {
	if c.closed.CompareAndSwap(false, true) {
		close(c.done)
	}
}

// Item accessors ─────────────────────────────────────────────────────────────

func (i *Item[V]) GetDuration() time.Duration { return i.d }
func (i *Item[V]) GetTime() time.Time         { return i.t }
func (i *Item[V]) GetValue() V                { return i.v }

// Options builder ────────────────────────────────────────────────────────────

func (o Options[K, V]) SetTimerResolution(d time.Duration) Options[K, V] {
	o.defaultResolution = d
	return o
}

func (o Options[K, V]) SetDefaultTTL(d time.Duration) Options[K, V] {
	o.defaultTTL = d
	return o
}

func (o Options[K, V]) SetDeallocationFunc(f DeallocationFunc[K, V]) Options[K, V] {
	o.deallocationFunc = f
	return o
}

func (o Options[K, V]) DisableUpdateTime(val bool) Options[K, V] {
	o.noUpdateTime = val
	return o
}

// SetShards sets the number of map shards.  Must be > 0; will be rounded up to
// the next power of 2.  Defaults to 16 when not set.  Higher values reduce
// mutex contention under concurrent workloads at the cost of slightly more
// memory (one map header + RWMutex per extra shard).
func (o Options[K, V]) SetShards(n int) Options[K, V] {
	o.shards = n
	return o
}

// fixupDuration maps the DefaultTTL sentinel to NoTTL when no default is set.
func (c *Cache[K, V]) fixupDuration(duration time.Duration) time.Duration {
	if duration == DefaultTTL {
		return c.o.defaultTTL
	}
	return duration
}
