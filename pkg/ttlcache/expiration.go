// Copyright (c) 2021-2025 autogrr contributors
// SPDX-License-Identifier: MIT

package ttlcache

import (
	"time"
)

func (c *Cache[K, V]) startExpirations() {
	timer := time.NewTimer(1 * time.Second)
	stopTimer(timer) // stop immediately; acts as initialised sentinel
	defer stopTimer(timer)

	// perShard tracks the earliest known expiry deadline for each shard.
	// The expiry goroutine only locks a shard when its slot is due — zero means
	// no known upcoming deadline for that shard.
	perShard := make([]time.Time, len(c.shards))

	// nextWake is the globally earliest deadline across all shards and drives
	// the single shared timer.
	var nextWake time.Time

	for {
		select {
		case sw, ok := <-c.ch:
			if !ok {
				return
			}
			if sw.t.IsZero() {
				continue // NoTTL item — nothing to schedule
			}

			// Update per-shard earliest.
			if perShard[sw.idx].IsZero() || perShard[sw.idx].After(sw.t) {
				perShard[sw.idx] = sw.t
			}

			// Only restart the timer if this deadline is earlier than the
			// current global target.
			if nextWake.IsZero() || nextWake.After(sw.t) {
				nextWake = sw.t
				restartTimer(timer, nextWake.Sub(c.tc.Now()))
			}

		case <-timer.C:
			stopTimer(timer)
			now := c.tc.Now()
			nextWake = time.Time{}

			for i := range c.shards {
				sh := perShard[i]
				if sh.IsZero() {
					continue
				}
				if sh.After(now) {
					// Not due yet — carry forward for next wake.
					if nextWake.IsZero() || nextWake.After(sh) {
						nextWake = sh
					}
					continue
				}
				// Shard i has items due — sweep only this shard.
				perShard[i] = c.expireShard(i, now)
				// Sync the atomic min so notify() has up-to-date information.
				var minNano int64
				if !perShard[i].IsZero() {
					minNano = perShard[i].UnixNano()
				}
				c.perShardMin[i].Store(minNano)
				if !perShard[i].IsZero() {
					if nextWake.IsZero() || nextWake.After(perShard[i]) {
						nextWake = perShard[i]
					}
				}
			}

			if !nextWake.IsZero() {
				restartTimer(timer, nextWake.Sub(c.tc.Now()))
			}
		}
	}
}

func restartTimer(t *time.Timer, d time.Duration) {
	stopTimer(t)
	t.Reset(d)
}

// stopTimer stops t and drains any pending tick from the channel.
// This is necessary because Timer.Stop() does not drain the channel; a
// subsequent Reset() could fire immediately on a stale tick.
func stopTimer(t *time.Timer) {
	t.Stop()
	if len(t.C) != 0 {
		<-t.C
	}
}

// expireShard sweeps shard idx for items whose deadline <= now, deletes them,
// and returns the earliest FUTURE deadline remaining in that shard (zero if
// none).  Only the targeted shard is locked — all other shards remain
// uncontested.
func (c *Cache[K, V]) expireShard(idx int, now time.Time) time.Time {
	var soon time.Time
	cm := &c.shards[idx]
	cm.Lock()
	for k, v := range cm.m {
		if v.t.IsZero() {
			continue
		}
		if v.t.After(now) {
			if soon.IsZero() || soon.After(v.t) {
				soon = v.t
			}
			continue
		}
		c.deleteUnsafe(cm, k, v, ReasonTimedOut)
	}
	cm.Unlock()
	return soon
}
