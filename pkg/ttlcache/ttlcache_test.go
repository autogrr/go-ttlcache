// Copyright (c) 2021-2025 autogrr contributors
// SPDX-License-Identifier: MIT

package ttlcache

import (
	"sync/atomic"
	"testing"
	"time"
)

// ── correctness tests ────────────────────────────────────────────────────────

func TestGet(t *testing.T) {
	t.Parallel()
	c := New[int, bool](Options[int, bool]{}.SetDefaultTTL(1 * time.Second))
	defer c.Close()

	for i := 0; i < 10; i++ {
		c.Set(i, true, DefaultTTL)
	}

	for i := 0; i < 10; i++ {
		val, ok := c.Get(i)
		if !ok {
			t.Fatalf("missing key: %d", i)
		} else if !val {
			t.Fatalf("bad value on key: %d", i)
		}
	}
}

func TestExpirations(t *testing.T) {
	t.Parallel()
	c := New[int, bool](Options[int, bool]{}.SetDefaultTTL(200 * time.Millisecond))
	defer c.Close()
	for i := 0; i < 10; i++ {
		c.Set(i, true, DefaultTTL)
	}

	time.Sleep(1 * time.Second)

	for i := 0; i < 10; i++ {
		if _, ok := c.Get(i); ok {
			t.Fatalf("found key: %d", i)
		}
	}
}

func TestSwaps(t *testing.T) {
	t.Parallel()
	c := New[int, bool](Options[int, bool]{}.SetDefaultTTL(200 * time.Millisecond))
	defer c.Close()
	for i := 0; i < 10; i++ {
		c.Set(i, true, DefaultTTL)
	}

	time.Sleep(1 * time.Second)
	for i := 0; i < 10; i++ {
		if _, ok := c.Get(i); ok {
			t.Fatalf("found key: %d", i)
		}
	}

	for i := 10; i < 20; i++ {
		c.Set(i, true, DefaultTTL)
		if _, ok := c.Get(i); !ok {
			t.Fatalf("missing key: %d", i)
		}
	}
}

func TestRetimer(t *testing.T) {
	t.Parallel()
	c := New[int, bool](Options[int, bool]{}.SetDefaultTTL(200 * time.Millisecond))
	defer c.Close()
	for i := 1; i < 10; i++ {
		c.Set(i, true, time.Duration(10-i)*100*time.Millisecond)
	}

	time.Sleep(2 * time.Second)
	for i := 1; i < 10; i++ {
		if _, ok := c.Get(i); ok {
			t.Fatalf("found key: %d", i)
		}
	}
}

func TestSchedule(t *testing.T) {
	t.Parallel()
	c := New[int, bool](Options[int, bool]{}.SetDefaultTTL(1 * time.Second))
	defer c.Close()
	for i := 1; i < 10; i++ {
		c.Set(i, true, time.Duration(i)*100*time.Millisecond)
	}

	time.Sleep(3 * time.Second)
	for i := 1; i < 10; i++ {
		if _, ok := c.Get(i); ok {
			t.Fatalf("found key: %d", i)
		}
	}
}

func TestInterlace(t *testing.T) {
	t.Parallel()
	c := New[int, bool](Options[int, bool]{}.SetDefaultTTL(100 * time.Millisecond))
	defer c.Close()
	swap := false
	for i := 0; i < 10; i++ {
		swap = !swap
		ttl := DefaultTTL
		if swap {
			ttl = NoTTL
		}
		c.Set(i, true, ttl)
	}

	time.Sleep(1 * time.Second)
	swap = false
	for i := 0; i < 10; i++ {
		swap = !swap
		if !swap {
			continue
		}

		if _, ok := c.Get(i); !ok {
			t.Fatalf("found key: %d", i)
		}
	}
}

func TestReschedule(t *testing.T) {
	t.Parallel()
	c := New[int, bool](Options[int, bool]{}.SetDefaultTTL(100 * time.Millisecond))
	defer c.Close()
	for i := 1; i < 10; i++ {
		c.Set(i, true, NoTTL)
		c.Set(i, true, DefaultTTL)
	}

	time.Sleep(1 * time.Second)
	for i := 1; i < 10; i++ {
		if _, ok := c.Get(i); ok {
			t.Fatalf("found key: %d", i)
		}
	}
}

func TestRescheduleNoTTL(t *testing.T) {
	t.Parallel()
	c := New[int, bool](Options[int, bool]{}.SetDefaultTTL(100 * time.Millisecond))
	defer c.Close()
	for i := 1; i < 10; i++ {
		c.Set(i, true, DefaultTTL)
		c.Set(i, true, NoTTL)
	}

	time.Sleep(1 * time.Second)
	for i := 1; i < 10; i++ {
		if _, ok := c.Get(i); !ok {
			t.Fatalf("found key: %d", i)
		}
	}
}

func TestDelete(t *testing.T) {
	t.Parallel()
	c := New[int, bool](Options[int, bool]{}.SetDefaultTTL(100 * time.Millisecond))
	defer c.Close()
	for i := 1; i < 10; i++ {
		c.Set(i, true, NoTTL)
		c.Delete(i)
	}

	for i := 1; i < 10; i++ {
		if _, ok := c.Get(i); ok {
			t.Fatalf("found key: %d", i)
		}
	}
}

func TestDeallocationTimeout(t *testing.T) {
	t.Parallel()
	var hit atomic.Bool
	o := Options[int, bool]{}.
		SetDefaultTTL(time.Millisecond * 100).
		SetDeallocationFunc(func(key int, value bool, reason DeallocationReason) {
			if reason == ReasonTimedOut {
				hit.Store(true)
			}
		})

	c := New[int, bool](o)
	defer c.Close()

	c.Set(0, true, DefaultTTL)

	time.Sleep(3 * time.Second)
	if !hit.Load() {
		t.Fatalf("Deallocation not hit.")
	}
}

func TestDeallocationDeleted(t *testing.T) {
	t.Parallel()
	var hit atomic.Bool
	o := Options[int, bool]{}.
		SetDefaultTTL(time.Millisecond * 100).
		SetDeallocationFunc(func(key int, value bool, reason DeallocationReason) {
			if reason == ReasonDeleted {
				hit.Store(true)
			}
		})

	c := New[int, bool](o)
	defer c.Close()

	c.Set(0, true, DefaultTTL)
	c.Delete(0)

	if !hit.Load() {
		t.Fatalf("Deallocation not hit.")
	}
}

func TestTimerReset(t *testing.T) {
	t.Parallel()
	ch := make(chan struct{})
	defer close(ch)

	c := New[int, bool](Options[int, bool]{}.
		SetDefaultTTL(time.Millisecond * 100).
		SetDeallocationFunc(func(key int, value bool, reason DeallocationReason) { ch <- struct{}{} }))

	defer c.Close()

	const base = 0
	const rounds = 1
	for i := base; i < rounds; i++ {
		c.Set(i, true, DefaultTTL)
	}

	for i := base; i < rounds; i++ {
		<-ch
	}

	for i := 0; i < 1; i++ {
		c.Set(i, true, DefaultTTL)
	}

	for i := base; i < rounds; i++ {
		<-ch
	}
}

func TestGetOrSet(t *testing.T) {
	t.Parallel()
	c := New[string, int](Options[string, int]{}.SetDefaultTTL(time.Second))
	defer c.Close()

	v, ok := c.GetOrSet("a", 1, DefaultTTL)
	if !ok || v != 1 {
		t.Fatalf("expected (1, true), got (%d, %v)", v, ok)
	}

	v, ok = c.GetOrSet("a", 99, DefaultTTL)
	if !ok || v != 1 {
		t.Fatalf("expected cached value 1, got (%d, %v)", v, ok)
	}
}

func TestNoTTLNeverExpires(t *testing.T) {
	t.Parallel()
	c := New[int, string](Options[int, string]{}.SetDefaultTTL(50 * time.Millisecond))
	defer c.Close()

	c.Set(1, "forever", NoTTL)
	time.Sleep(500 * time.Millisecond)

	v, ok := c.Get(1)
	if !ok || v != "forever" {
		t.Fatalf("NoTTL item should never expire, got (%q, %v)", v, ok)
	}
}

func TestGetKeys(t *testing.T) {
	t.Parallel()
	c := New[int, bool](Options[int, bool]{}.SetDefaultTTL(time.Second))
	defer c.Close()

	for i := 0; i < 5; i++ {
		c.Set(i, true, NoTTL)
	}

	keys := c.GetKeys()
	if len(keys) != 5 {
		t.Fatalf("expected 5 keys, got %d", len(keys))
	}
}

func TestItemAccessorsDefaultTTL(t *testing.T) {
	t.Parallel()
	const ttl = 500 * time.Millisecond
	c := New[string, int](Options[string, int]{}.SetDefaultTTL(ttl))
	defer c.Close()

	it := c.SetItem("x", 42, DefaultTTL)
	if it.GetValue() != 42 {
		t.Fatalf("expected value 42, got %d", it.GetValue())
	}
	if it.GetDuration() != ttl {
		t.Fatalf("expected duration %v, got %v", ttl, it.GetDuration())
	}
	if it.GetTime().IsZero() {
		t.Fatal("expected non-zero expiry time")
	}
}

func TestDisableUpdateTime(t *testing.T) {
	t.Parallel()
	c := New[int, bool](Options[int, bool]{}.
		SetDefaultTTL(200 * time.Millisecond).
		DisableUpdateTime(true))
	defer c.Close()

	c.Set(1, true, DefaultTTL)
	it1, _ := c.GetItem(1)
	time.Sleep(10 * time.Millisecond)
	it2, _ := c.GetItem(1)

	if it1.GetTime() != it2.GetTime() {
		t.Fatal("DisableUpdateTime: expiry should not change on Get")
	}
}

// ── benchmarks ───────────────────────────────────────────────────────────────

func BenchmarkSet(b *testing.B) {
	c := New[int, int](Options[int, int]{}.SetDefaultTTL(time.Minute))
	defer c.Close()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.Set(i, i, DefaultTTL)
	}
}

func BenchmarkSetNoTTL(b *testing.B) {
	c := New[int, int](Options[int, int]{})
	defer c.Close()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.Set(i, i, NoTTL)
	}
}

func BenchmarkGet(b *testing.B) {
	const size = 1024
	c := New[int, int](Options[int, int]{}.SetDefaultTTL(time.Minute))
	defer c.Close()
	for i := 0; i < size; i++ {
		c.Set(i, i, DefaultTTL)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = c.Get(i % size)
	}
}

func BenchmarkGetNoUpdateTime(b *testing.B) {
	const size = 1024
	c := New[int, int](Options[int, int]{}.
		SetDefaultTTL(time.Minute).
		DisableUpdateTime(true))
	defer c.Close()
	for i := 0; i < size; i++ {
		c.Set(i, i, DefaultTTL)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = c.Get(i % size)
	}
}

func BenchmarkGetMiss(b *testing.B) {
	c := New[int, int](Options[int, int]{}.SetDefaultTTL(time.Minute))
	defer c.Close()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = c.Get(i)
	}
}

func BenchmarkGetOrSet(b *testing.B) {
	const size = 1024
	c := New[int, int](Options[int, int]{}.SetDefaultTTL(time.Minute))
	defer c.Close()
	for i := 0; i < size; i++ {
		c.Set(i, i, DefaultTTL)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = c.GetOrSet(i%size, i, DefaultTTL)
	}
}

func BenchmarkSetParallel(b *testing.B) {
	c := New[int, int](Options[int, int]{}.SetDefaultTTL(time.Minute))
	defer c.Close()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			c.Set(i, i, DefaultTTL)
			i++
		}
	})
}

func BenchmarkGetParallel(b *testing.B) {
	const size = 1024
	c := New[int, int](Options[int, int]{}.SetDefaultTTL(time.Minute))
	defer c.Close()
	for i := 0; i < size; i++ {
		c.Set(i, i, DefaultTTL)
	}
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			_, _ = c.Get(i % size)
			i++
		}
	})
}

func BenchmarkGetNoUpdateTimeParallel(b *testing.B) {
	const size = 1024
	c := New[int, int](Options[int, int]{}.
		SetDefaultTTL(time.Minute).
		DisableUpdateTime(true))
	defer c.Close()
	for i := 0; i < size; i++ {
		c.Set(i, i, DefaultTTL)
	}
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			_, _ = c.Get(i % size)
			i++
		}
	})
}

func BenchmarkMixedReadWrite(b *testing.B) {
	const size = 1024
	c := New[int, int](Options[int, int]{}.SetDefaultTTL(time.Minute))
	defer c.Close()
	for i := 0; i < size; i++ {
		c.Set(i, i, DefaultTTL)
	}
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			if i%5 == 0 {
				c.Set(i%size, i, DefaultTTL)
			} else {
				_, _ = c.Get(i % size)
			}
			i++
		}
	})
}

func BenchmarkDeleteAndSet(b *testing.B) {
	const size = 256
	c := New[int, int](Options[int, int]{}.SetDefaultTTL(time.Minute))
	defer c.Close()
	for i := 0; i < size; i++ {
		c.Set(i, i, DefaultTTL)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := i % size
		c.Delete(key)
		c.Set(key, i, DefaultTTL)
	}
}
