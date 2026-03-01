// Copyright (c) 2021-2025 autogrr contributors
// SPDX-License-Identifier: MIT

package timecache

import (
	"testing"
	"time"
)

func TestTime(t *testing.T) {
	t.Parallel()
	tc := (&Cache{}).Now()
	if tc.IsZero() {
		t.Fatalf("time is zero")
	}
}

func TestRounding(t *testing.T) {
	t.Parallel()
	ti := New(Options{}.Round(time.Minute * 5)).Now()

	if ti.Minute()%5 != 0 {
		t.Fatalf("time is not a 5 multiple")
	}
}

func TestResolution(t *testing.T) {
	t.Parallel()
	const magicNumber = 3
	const rounds = 700
	ti := New(Options{}.Round(time.Millisecond * magicNumber))

	unique := 0
	old := ti.Now().UnixMilli()
	for i := 0; i < rounds; i++ {
		new := ti.Now().UnixMilli()
		if new > old {
			unique++
			old = new
		}

		if div := new % magicNumber; div != 0 {
			t.Fatalf("not a multiple of %d: %d", magicNumber, div)
		}
		time.Sleep(time.Millisecond * 1)
	}

	if unique < rounds/magicNumber-1 {
		t.Fatalf("not enough resolution rounds %d", unique)
	}
}

// BenchmarkNow measures the cost of a cached Now() call (cache hit path).
func BenchmarkNow(b *testing.B) {
	tc := New(Options{}.Round(time.Second))
	_ = tc.Now() // warm up
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = tc.Now()
	}
}

// BenchmarkNowParallel measures concurrent cached Now() reads.
func BenchmarkNowParallel(b *testing.B) {
	tc := New(Options{}.Round(time.Second))
	_ = tc.Now() // warm up
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_ = tc.Now()
		}
	})
}

// BenchmarkNowHighResolution benchmarks a frequently-expiring cache (1 ms period).
func BenchmarkNowHighResolution(b *testing.B) {
	tc := New(Options{}.Round(time.Millisecond))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = tc.Now()
	}
}
