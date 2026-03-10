// Copyright (c) 2021-2025 autogrr contributors
// SPDX-License-Identifier: MIT

package timecache

import (
	"sync"
	"sync/atomic"
	"time"
)

// Cache returns a rounded, cached snapshot of time.Now().
// Reads on the hot path are a single atomic load (~2 ns).
// A sync.Mutex protects the single goroutine that recomputes the value.
type Cache struct {
	t  atomic.Pointer[time.Time]
	mu sync.Mutex
	o  Options
}

// Options configures the rounding resolution of the cache.
type Options struct {
	round time.Duration
}

// Round sets the rounding resolution.  The cached value is refreshed every
// round/2 so callers observe a clock that advances in steps of round.
func (o Options) Round(d time.Duration) Options {
	o.round = d
	return o
}

// New returns an initialised *Cache with the given options.
func New(o Options) *Cache {
	return &Cache{o: o}
}

// Now returns a cached, rounded snapshot of the current time.
// The cache is invalidated every round/2 (or every 500 ms when no round is set).
func (c *Cache) Now() time.Time {
	if p := c.t.Load(); p != nil {
		return *p
	}
	return c.update()
}

func (c *Cache) update() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Double-check: another goroutine may have filled it while we waited.
	if p := c.t.Load(); p != nil {
		return *p
	}

	d := c.o.round
	if d <= time.Nanosecond {
		d = time.Second
	}

	t := time.Now().Round(d)
	c.t.Store(&t)

	// Sleep for half the period so callers see a clock that advances at the
	// configured resolution.  When using the default 1 s period, we wake at
	// 500 ms — still well within the 1 s resolution guarantee.
	go func(c *Cache, sleep time.Duration) {
		time.Sleep(sleep)
		c.t.Store(nil)
	}(c, d/2)

	return t
}
