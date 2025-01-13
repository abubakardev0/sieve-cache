// Package sieve provides a probabilistic cache implementation with ghost entries
// and TTL support. These tests verify the cache's functionality, performance,
// and thread-safety.
package sieve

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"
)

// TestNewSieveCacheWithConfig verifies cache initialization with various
// configurations, ensuring proper validation of input parameters and
// handling of edge cases.
func TestNewSieveCacheWithConfig(t *testing.T) {
	tests := []struct {
		name    string
		config  Config[string, string]
		wantErr bool
	}{
		{
			name: "valid config",
			config: Config[string, string]{
				Capacity:       1024,
				GhostSize:      10,
				AdmitThreshold: 0.3,
				MaxEntrySize:   100,
				TTL:            time.Hour,
			},
			wantErr: false,
		},
		{
			name: "invalid capacity",
			config: Config[string, string]{
				Capacity:       -1,
				GhostSize:      10,
				AdmitThreshold: 0.3,
			},
			wantErr: true,
		},
		{
			name: "invalid ghost size",
			config: Config[string, string]{
				Capacity:       1024,
				GhostSize:      -1,
				AdmitThreshold: 0.3,
			},
			wantErr: true,
		},
		{
			name: "invalid admit threshold",
			config: Config[string, string]{
				Capacity:       1024,
				GhostSize:      10,
				AdmitThreshold: 1.5,
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewSieveCacheWithConfig(tt.config)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewSieveCacheWithConfig() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

// TestSieveCache_Basic verifies the fundamental operations of the cache
// including Set and Get operations, ensuring basic functionality works
// as expected.
func TestSieveCache_Basic(t *testing.T) {
	t.Log("Starting basic cache test...")
	start := time.Now()

	cache, err := NewSieveCacheWithConfig(Config[string, string]{
		Capacity:       1024,
		GhostSize:      10,
		AdmitThreshold: 0.3,
		Logger:         slog.Default(),
	})
	if err != nil {
		t.Fatalf("Failed to create cache: %v", err)
	}
	defer func() {
		cache.Close()
		t.Logf("Test completed in %v", time.Since(start))
	}()

	ctx := context.Background()

	// Test Set operation
	t.Log("Testing Set operation...")
	err = cache.Set(ctx, "key1", "value1", 10)
	if err != nil {
		t.Errorf("Set() error = %v", err)
	}
	t.Log("Set operation successful")

	// Test Get operation
	t.Log("Testing Get operation...")
	if value, found := cache.Get(ctx, "key1"); !found {
		t.Error("Get() failed to retrieve value")
	} else {
		t.Logf("Get operation successful, value: %v", value)
	}

	// Log cache statistics
	stats := cache.GetStats()
	t.Logf("Cache statistics: hits=%d, misses=%d, evictions=%d",
		stats.Hits, stats.Misses, stats.EvictionCount)
}

// TestSieveCache_Eviction verifies the cache's eviction policy when it
// reaches capacity, ensuring proper removal of entries and maintenance
// of size constraints.
func TestSieveCache_Eviction(t *testing.T) {
	cache, err := NewSieveCacheWithConfig(Config[string, string]{
		Capacity:       100,
		GhostSize:      5,
		AdmitThreshold: 0.3,
		Logger:         slog.Default(),
	})
	if err != nil {
		t.Fatalf("Failed to create cache: %v", err)
	}
	defer cache.Close()

	ctx := context.Background()

	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("key%d", i)
		value := strings.Repeat("x", 20)
		err = cache.Set(ctx, key, value, 20)
		if err != nil {
			t.Errorf("Set() error = %v", err)
		}
	}

	stats := cache.GetStats()
	if stats.CurrentSize > stats.Capacity {
		t.Errorf("Cache size %d exceeds capacity %d", stats.CurrentSize, stats.Capacity)
	}
}

// TestSieveCache_TTL verifies the time-to-live functionality of cached
// entries, ensuring proper expiration and removal of stale entries.
func TestSieveCache_TTL(t *testing.T) {
	cache, err := NewSieveCacheWithConfig(Config[string, string]{
		Capacity:       1024,
		GhostSize:      10,
		AdmitThreshold: 0.3,
		TTL:            100 * time.Millisecond,
		Logger:         slog.Default(),
	})
	if err != nil {
		t.Fatalf("Failed to create cache: %v", err)
	}
	defer cache.Close()

	ctx := context.Background()

	err = cache.Set(ctx, "key1", "value1", 10)
	if err != nil {
		t.Errorf("Set() error = %v", err)
	}

	if _, found := cache.Get(ctx, "key1"); !found {
		t.Error("Get() couldn't find recently set key")
	}

	time.Sleep(200 * time.Millisecond)

	if _, found := cache.Get(ctx, "key1"); found {
		t.Error("Get() found expired key")
	}
}

// TestSieveCache_Concurrent verifies thread-safety of the cache under
// concurrent access from multiple goroutines, ensuring data consistency
// and proper synchronization.
func TestSieveCache_Concurrent(t *testing.T) {
	t.Log("Starting concurrent cache test...")
	start := time.Now()

	cache, err := NewSieveCacheWithConfig(Config[string, string]{
		Capacity:       1024,
		GhostSize:      10,
		AdmitThreshold: 0.3,
		Logger:         slog.Default(),
	})
	if err != nil {
		t.Fatalf("Failed to create cache: %v", err)
	}
	defer func() {
		cache.Close()
		t.Logf("Concurrent test completed in %v", time.Since(start))
	}()

	ctx := context.Background()
	const goroutines = 10
	const operationsPerGoroutine = 100

	var wg sync.WaitGroup
	wg.Add(goroutines)

	t.Logf("Starting %d goroutines with %d operations each",
		goroutines, operationsPerGoroutine)

	for i := 0; i < goroutines; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < operationsPerGoroutine; j++ {
				key := fmt.Sprintf("key-%d-%d", id, j)
				value := fmt.Sprintf("value-%d-%d", id, j)

				if j%2 == 0 {
					if err := cache.Set(ctx, key, value, int64(len(value))); err != nil {
						t.Errorf("Set() error = %v", err)
					}
				} else {
					cache.Get(ctx, key)
				}
			}
		}(i)
	}

	wg.Wait()
	stats := cache.GetStats()
	t.Logf("Final cache statistics: hits=%d, misses=%d, evictions=%d",
		stats.Hits, stats.Misses, stats.EvictionCount)
}

// TestSieveCache_GhostCache verifies the ghost cache functionality,
// ensuring proper tracking of recently evicted entries and their
// influence on readmission probability.
func TestSieveCache_GhostCache(t *testing.T) {
	cache, err := NewSieveCacheWithConfig(Config[string, string]{
		Capacity:       50,
		GhostSize:      5,
		AdmitThreshold: 0.3,
		Logger:         slog.Default(),
	})
	if err != nil {
		t.Fatalf("Failed to create cache: %v", err)
	}
	defer cache.Close()

	ctx := context.Background()

	// Fill cache to trigger evictions
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("key%d", i)
		value := strings.Repeat("x", 10)
		err = cache.Set(ctx, key, value, 10)
		if err != nil {
			t.Errorf("Set() error = %v", err)
		}
	}

	stats := cache.GetStats()
	if stats.GhostCount == 0 {
		t.Error("Expected ghost entries after eviction")
	}
}

// TestSieveCache_Clear verifies the cache clearing functionality,
// ensuring all entries are properly removed and resources are freed.
func TestSieveCache_Clear(t *testing.T) {
	cache, err := NewSieveCacheWithConfig(Config[string, string]{
		Capacity:       1024,
		GhostSize:      10,
		AdmitThreshold: 0.3,
		Logger:         slog.Default(),
	})
	if err != nil {
		t.Fatalf("Failed to create cache: %v", err)
	}
	defer cache.Close()

	ctx := context.Background()

	// Add some entries
	err = cache.Set(ctx, "key1", "value1", 10)
	if err != nil {
		t.Errorf("Set() error = %v", err)
	}

	// Clear the cache
	cache.Clear()

	// Verify cache is empty
	stats := cache.GetStats()
	if stats.CurrentSize != 0 || stats.ItemCount != 0 {
		t.Error("Cache not empty after Clear()")
	}
}

// BenchmarkSieveCache_Set measures the performance of cache Set
// operations under load, providing metrics for write performance.
func BenchmarkSieveCache_Set(b *testing.B) {
	cache, _ := NewSieveCacheWithConfig(Config[string, string]{
		Capacity:       1024 * 1024,
		GhostSize:      100,
		AdmitThreshold: 0.3,
		Logger:         slog.Default(),
	})
	defer cache.Close()

	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key-%d", i)
		value := fmt.Sprintf("value-%d", i)
		cache.Set(ctx, key, value, int64(len(value)))
	}
}

// BenchmarkSieveCache_Get measures the performance of cache Get
// operations under load, providing metrics for read performance.
func BenchmarkSieveCache_Get(b *testing.B) {
	cache, _ := NewSieveCacheWithConfig(Config[string, string]{
		Capacity:       1024 * 1024,
		GhostSize:      100,
		AdmitThreshold: 0.3,
		Logger:         slog.Default(),
	})
	defer cache.Close()

	ctx := context.Background()

	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("key-%d", i)
		value := fmt.Sprintf("value-%d", i)
		cache.Set(ctx, key, value, int64(len(value)))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key-%d", i%1000)
		cache.Get(ctx, key)
	}
}
