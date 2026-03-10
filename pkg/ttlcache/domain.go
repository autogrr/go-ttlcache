// Copyright (c) 2021-2025 autogrr contributors
// SPDX-License-Identifier: MIT

package ttlcache

import (
	"hash/maphash"
	"sync"
	"sync/atomic"
	"time"

	"github.com/autogrr/go-ttlcache/pkg/timecache"
)

// NoTTL means the item never expires.
const NoTTL time.Duration = 0

// DefaultTTL is a sentinel requesting the cache-wide default TTL.
const DefaultTTL time.Duration = time.Nanosecond * 1

// defaultShards is the number of map shards used when none is specified.
// Must be a power of 2.  16 stripes reduce single-mutex contention ~16x under
// concurrent workloads while keeping memory overhead negligible.
const defaultShards = 16

// shardWakeup is sent through the expiry channel to tell the expiry goroutine
// which shard has an upcoming deadline and when.
type shardWakeup struct {
	idx int
	t   time.Time
}

// cacheMap is one shard: an independent map protected by its own RWMutex.
// Modelled after url.Values — the type owns its data and its lock.
// idx is the position in Cache.shards and is set once by New.
//
// The struct is padded to 128 bytes (two cache lines) so that adjacent shards
// in the slice never share a cache line, eliminating false sharing between
// goroutines operating on different shards simultaneously.
//
// Size breakdown (amd64): sync.RWMutex=24 + map ptr=8 + idx=8 = 40 → pad 88.
type cacheMap[K comparable, V any] struct {
	sync.RWMutex
	m   map[K]Item[V]
	idx int
}

// load returns the item for key under a read lock.
func (cm *cacheMap[K, V]) load(key K) (Item[V], bool) {
	cm.RLock()
	v, ok := cm.m[key]
	cm.RUnlock()
	return v, ok
}

// Cache is a generic, goroutine-safe sharded in-memory cache with per-item TTL
// expiry.  The keyspace is split across shards, each protected by its own
// RWMutex, so concurrent operations on different keys never contend.
type Cache[K comparable, V any] struct {
	tc     *timecache.Cache // pointer — must not be copied after first use
	shards []cacheMap[K, V] // len is always a power of 2
	seed   maphash.Seed     // stable per-instance hash seed
	o      Options[K, V]
	ch     chan shardWakeup
	done   chan struct{} // closed by Close(); signals expiry goroutine to stop
	closed atomic.Bool   // set by Close(); fast-path guard for notify()
}

// shard returns the cacheMap responsible for key using a bitmask.
// len(c.shards) is always a power of 2, so & is equivalent to % but branchless.
func (c *Cache[K, V]) shard(key K) *cacheMap[K, V] {
	h := maphash.Comparable(c.seed, key)
	return &c.shards[h&uint64(len(c.shards)-1)]
}

// Item holds a cached value together with its expiry metadata.
type Item[V any] struct {
	t time.Time
	d time.Duration
	v V
}

// Options configures a Cache.
type Options[K comparable, V any] struct {
	defaultTTL        time.Duration
	defaultResolution time.Duration
	deallocationFunc  DeallocationFunc[K, V]
	noUpdateTime      bool
	shards            int // must be a power of 2; 0 → defaultShards
}

// DeallocationReason describes why an item was removed from the cache.
type DeallocationReason int

const (
	ReasonTimedOut = DeallocationReason(iota)
	ReasonDeleted  = DeallocationReason(iota)
)

// DeallocationFunc is called whenever an item is removed from the cache.
type DeallocationFunc[K comparable, V any] func(key K, value V, reason DeallocationReason)

// nextPow2 returns the smallest power of 2 >= n.
func nextPow2(n int) int {
	n--
	n |= n >> 1
	n |= n >> 2
	n |= n >> 4
	n |= n >> 8
	n |= n >> 16
	n++
	return n
}
