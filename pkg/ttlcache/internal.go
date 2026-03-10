// Copyright (c) 2021-2025 autogrr contributors
// SPDX-License-Identifier: MIT

package ttlcache

import "time"

// ── unsafe shard ops (caller must hold the shard lock) ───────────────────────

// _s stores it in shard cm and enqueues the expiry time.
// Caller must hold cm.Lock().
func (c *Cache[K, V]) _s(cm *cacheMap[K, V], key K, it Item[V]) Item[V] {
	it.d, it.t = c.getDuration(it.d)
	cm.m[key] = it
	return it
}

// _gos is an atomic get-or-set inside cm.
// Caller must hold cm.Lock().
func (c *Cache[K, V]) _gos(cm *cacheMap[K, V], key K, it Item[V]) (Item[V], bool) {
	if g, ok := cm.m[key]; ok {
		return g, true
	}
	return c._s(cm, key, it), true
}

// deleteUnsafe removes key from cm.
// Caller must hold cm.Lock().
// The deallocation callback is NOT called here — the caller must fire it
// after releasing the lock to avoid deadlocks (the callback may block or
// re-enter the cache).
func (c *Cache[K, V]) deleteUnsafe(cm *cacheMap[K, V], key K) {
	delete(cm.m, key)
}

// notify tells the expiry goroutine about a new deadline for shard cm.
//
// The send is non-blocking: if the buffer is full the expiry goroutine is
// already behind and will sweep the shard on its next wake anyway.
// Using a select with a default case also prevents a deadlock that would
// arise if notify() blocked on a full channel while holding the shard lock —
// the expiry goroutine acquires the same lock in expireShard, so a blocking
// send would create a cycle.
func (c *Cache[K, V]) notify(cm *cacheMap[K, V], t time.Time) {
	if t.IsZero() || c.closed.Load() {
		return
	}

	select {
	case c.ch <- shardWakeup{idx: cm.idx, t: t}:
	case <-c.done:
	}
}

// ── locking wrappers ─────────────────────────────────────────────────────────

func (c *Cache[K, V]) get(key K) (Item[V], bool) {
	return c.shard(key).load(key)
}

func (c *Cache[K, V]) set(key K, it Item[V]) Item[V] {
	cm := c.shard(key)
	cm.Lock()
	it = c._s(cm, key, it)
	cm.Unlock()
	c.notify(cm, it.t)
	return it
}

func (c *Cache[K, V]) getOrSet(key K, it Item[V]) (Item[V], bool) {
	cm := c.shard(key)
	cm.Lock()
	result, ok := c._gos(cm, key, it)
	cm.Unlock()
	c.notify(cm, result.t)
	return result, ok
}

// delete removes key from the cache, calling the deallocation function if set.
// If the key does not exist the call is a no-op.
func (c *Cache[K, V]) delete(key K, reason DeallocationReason) {
	cm := c.shard(key)
	cm.Lock()
	v, ok := cm.m[key]
	if ok {
		c.deleteUnsafe(cm, key)
	}
	cm.Unlock()
	// Fire the callback outside the lock so it can safely re-enter the cache.
	if ok && c.o.deallocationFunc != nil {
		c.o.deallocationFunc(key, v.v, reason)
	}
}

func (c *Cache[K, V]) getkeys() []K {
	// First pass: count total keys to allocate exactly once.
	total := 0
	for i := range c.shards {
		cm := &c.shards[i]
		cm.RLock()
		total += len(cm.m)
		cm.RUnlock()
	}
	if total == 0 {
		return nil
	}
	// Second pass: collect.  The cache may have changed between passes;
	// append handles growth past total, and unused capacity is benign.
	keys := make([]K, 0, total)
	for i := range c.shards {
		cm := &c.shards[i]
		cm.RLock()
		for k := range cm.m {
			keys = append(keys, k)
		}
		cm.RUnlock()
	}
	return keys
}

// getDuration resolves d to a concrete (duration, expiryTime) pair.
//
//   - NoTTL      → (0, zero)   item never expires
//   - DefaultTTL → use Options.defaultTTL
//   - anything else → use d directly
func (c *Cache[K, V]) getDuration(d time.Duration) (time.Duration, time.Time) {
	switch d {
	case NoTTL:
		return NoTTL, time.Time{}
	case DefaultTTL:
		return c.o.defaultTTL, c.tc.Now().Add(c.o.defaultTTL)
	default:
		return d, c.tc.Now().Add(d)
	}
}
