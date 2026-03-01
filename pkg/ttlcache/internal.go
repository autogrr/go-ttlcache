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
	c.notify(cm, it.t)
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

// deleteUnsafe removes key from cm and fires the deallocation callback.
// Caller must hold cm.Lock().
func (c *Cache[K, V]) deleteUnsafe(cm *cacheMap[K, V], key K, v Item[V], reason DeallocationReason) {
	delete(cm.m, key)
	if c.o.deallocationFunc != nil {
		c.o.deallocationFunc(key, v.v, reason)
	}
}

// notify tells the expiry goroutine about a new deadline for shard cm.
//
// Two fast-exit paths avoid touching the channel at all:
//  1. closed flag — cache is shutting down.
//  2. Atomic minimum — the expiry goroutine already knows about an earlier or
//     equal deadline for this shard; this send would be redundant.
//
// The channel send is non-blocking: if the buffer is full the expiry goroutine
// is already backed up and will sweep the shard on its next wake anyway.
// In that case we reset the per-shard min so the next Set can resend.
func (c *Cache[K, V]) notify(cm *cacheMap[K, V], t time.Time) {
	if t.IsZero() || c.closed.Load() {
		return
	}
	nano := t.UnixNano()
	// CAS loop: update perShardMin only if this deadline is strictly earlier.
	// Because _s is called under the shard write lock, at most one goroutine
	// at a time reaches this point for a given shard, so the loop resolves in
	// one or two iterations at most.
	for {
		cur := c.perShardMin[cm.idx].Load()
		if cur != 0 && cur <= nano {
			return // expiry goroutine already has an earlier wake scheduled
		}
		if c.perShardMin[cm.idx].CompareAndSwap(cur, nano) {
			break
		}
	}

	c.ch <- shardWakeup{idx: cm.idx, t: t}
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
	return it
}

func (c *Cache[K, V]) getOrSet(key K, it Item[V]) (Item[V], bool) {
	cm := c.shard(key)
	cm.Lock()
	result, ok := c._gos(cm, key, it)
	cm.Unlock()
	return result, ok
}

// delete removes key from the cache, calling the deallocation function if set.
// If the key does not exist the call is a no-op.
func (c *Cache[K, V]) delete(key K, reason DeallocationReason) {
	cm := c.shard(key)
	cm.Lock()
	v, ok := cm.m[key]
	if ok {
		c.deleteUnsafe(cm, key, v, reason)
	}
	cm.Unlock()
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
