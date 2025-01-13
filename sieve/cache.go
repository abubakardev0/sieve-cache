// Package sieve implements a probabilistic cache with ghost entries and TTL support.
// It provides thread-safe operations, automatic cleanup of expired entries, and
// configurable admission control based on access patterns.
package sieve

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

// Error definitions for cache operations
var (
	ErrEntrySizeTooLarge = errors.New("entry size exceeds cache capacity")
	ErrInvalidConfig     = errors.New("invalid cache configuration")
	ErrCacheClosed       = errors.New("cache is closed")
)

// Config defines the configuration parameters for SieveCache initialization.
// It supports generic key-value types and various tuning parameters.
type Config[K comparable, V any] struct {
	Capacity       int64
	GhostSize      int
	AdmitThreshold float64
	Logger         *slog.Logger
	MaxEntrySize   int64
	TTL            time.Duration
}

func (c Config[K, V]) validate() error {
	switch {
	case c.Capacity <= 0:
		return fmt.Errorf("%w: capacity must be positive", ErrInvalidConfig)
	case c.GhostSize < 0:
		return fmt.Errorf("%w: ghost size must be non-negative", ErrInvalidConfig)
	case c.AdmitThreshold < 0 || c.AdmitThreshold > 1:
		return fmt.Errorf("%w: admit threshold must be between 0 and 1", ErrInvalidConfig)
	case c.MaxEntrySize > c.Capacity:
		return fmt.Errorf("%w: max entry size cannot exceed capacity", ErrInvalidConfig)
	}
	return nil
}

type entry[K comparable, V any] struct {
	key      K
	value    V
	size     int64
	accessed atomic.Int64
	created  time.Time
	freq     atomic.Int32
	expiry   time.Time
}

type ghostEntry struct {
	size    int64
	evicted time.Time
	freq    int32
}

// Stats represents cache performance metrics and current state.
type Stats struct {
	Hits          int64
	Misses        int64
	HitRate       float64
	CurrentSize   int64
	Capacity      int64
	ItemCount     int
	GhostCount    int
	EvictionCount int64
	Uptime        time.Duration
	OpsPerSecond  float64
}

// SieveCache implements a probabilistic cache with ghost entries and TTL support.
type SieveCache[K comparable, V any] struct {
	mu             sync.RWMutex
	items          map[K]*entry[K, V]
	ghostCache     map[K]*ghostEntry
	capacity       int64
	currentSize    atomic.Int64
	ghostSize      int
	hitCount       atomic.Int64
	missCount      atomic.Int64
	evictionCount  atomic.Int64
	admitThreshold float64
	logger         *slog.Logger
	maxEntrySize   int64
	ttl            time.Duration
	startTime      time.Time

	closeOnce    sync.Once
	closed       atomic.Bool
	shutdownChan chan struct{}
}

// NewSieveCacheWithConfig creates a new cache instance with the provided configuration.
// It validates the configuration and initializes the cache components.
func NewSieveCacheWithConfig[K comparable, V any](config Config[K, V]) (*SieveCache[K, V], error) {
	if err := config.validate(); err != nil {
		return nil, err
	}

	if config.Logger == nil {
		config.Logger = slog.Default()
	}

	cache := &SieveCache[K, V]{
		items:          make(map[K]*entry[K, V]),
		ghostCache:     make(map[K]*ghostEntry),
		capacity:       config.Capacity,
		ghostSize:      config.GhostSize,
		admitThreshold: config.AdmitThreshold,
		logger:         config.Logger,
		maxEntrySize:   config.MaxEntrySize,
		ttl:            config.TTL,
		startTime:      time.Now(),
		shutdownChan:   make(chan struct{}),
	}

	go cache.periodicCleanup()
	return cache, nil
}

// Close shuts down the cache and cleans up resources.
func (c *SieveCache[K, V]) Close() error {
	c.closeOnce.Do(func() {
		c.closed.Store(true)
		close(c.shutdownChan)
		c.Clear()
	})
	return nil
}

func (c *SieveCache[K, V]) periodicCleanup() {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-c.shutdownChan:
			return
		case <-ticker.C:
			c.cleanupExpiredEntries()
		}
	}
}

func (c *SieveCache[K, V]) cleanupExpiredEntries() {
	c.mu.Lock()
	defer c.mu.Unlock()

	now := time.Now()
	for key, entry := range c.items {
		if !entry.expiry.IsZero() && now.After(entry.expiry) {
			c.removeEntry(key)
		}
	}
}

// Get retrieves a value from the cache using the provided key.
// It returns the value and a boolean indicating whether the key exists.
func (c *SieveCache[K, V]) Get(ctx context.Context, key K) (V, bool) {
	if c.closed.Load() {
		var zero V
		return zero, false
	}

	select {
	case <-ctx.Done():
		var zero V
		return zero, false
	default:
		c.mu.RLock()
		entry, exists := c.items[key]
		c.mu.RUnlock()

		if !exists {
			c.missCount.Add(1)
			var zero V
			return zero, false
		}

		if !entry.expiry.IsZero() && time.Now().After(entry.expiry) {
			c.mu.Lock()
			if _, exists := c.items[key]; exists {
				c.removeEntry(key)
			}
			c.mu.Unlock()
			c.missCount.Add(1)
			var zero V
			return zero, false
		}

		entry.accessed.Store(time.Now().Unix())
		entry.freq.Add(1)
		c.hitCount.Add(1)

		return entry.value, true
	}
}

// Set adds or updates a key-value pair in the cache.
// It handles size constraints, TTL, and admission control.
func (c *SieveCache[K, V]) Set(ctx context.Context, key K, value V, size int64) error {
	if c.closed.Load() {
		return ErrCacheClosed
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		if size > c.capacity {
			return ErrEntrySizeTooLarge
		}

		if c.maxEntrySize > 0 && size > c.maxEntrySize {
			return fmt.Errorf("entry size %d exceeds maximum allowed size %d", size, c.maxEntrySize)
		}

		c.mu.Lock()
		defer c.mu.Unlock()

		if !c.shouldAdmit(key, size) {
			return nil
		}

		var expiry time.Time
		if c.ttl > 0 {
			expiry = time.Now().Add(c.ttl)
		}

		if existing, exists := c.items[key]; exists {
			c.currentSize.Add(-existing.size)
			delete(c.items, key)
		}

		c.evictEntries(size)

		newEntry := &entry[K, V]{
			key:     key,
			value:   value,
			size:    size,
			created: time.Now(),
			expiry:  expiry,
		}
		newEntry.accessed.Store(time.Now().Unix())
		newEntry.freq.Store(1)

		c.items[key] = newEntry
		c.currentSize.Add(size)

		return nil
	}
}

func (c *SieveCache[K, V]) evictEntries(requiredSize int64) {
	for c.currentSize.Load()+requiredSize > c.capacity {
		if len(c.items) == 0 {
			break
		}
		c.evictOne()
	}
}

func (c *SieveCache[K, V]) evictOne() {
	var oldestKey K
	var oldestAccess int64 = time.Now().Unix()
	var oldestEntry *entry[K, V]

	for key, entry := range c.items {
		accessed := entry.accessed.Load()
		if accessed < oldestAccess {
			oldestAccess = accessed
			oldestKey = key
			oldestEntry = entry
		}
	}

	if oldestEntry != nil {
		c.ghostCache[oldestKey] = &ghostEntry{
			size:    oldestEntry.size,
			evicted: time.Now(),
			freq:    oldestEntry.freq.Load(),
		}

		if len(c.ghostCache) > c.ghostSize {
			c.cleanupGhostCache()
		}

		c.removeEntry(oldestKey)
		c.evictionCount.Add(1)
	}
}

func (c *SieveCache[K, V]) removeEntry(key K) {
	if entry, exists := c.items[key]; exists {
		c.currentSize.Add(-entry.size)
		delete(c.items, key)
	}
}

func (c *SieveCache[K, V]) shouldAdmit(key K, size int64) bool {
	if ghost, exists := c.ghostCache[key]; exists {
		if ghost.freq > 1 {
			return true
		}
		if time.Since(ghost.evicted) < time.Hour {
			return ghost.freq > 0
		}
	}

	currentSize := c.currentSize.Load()
	baseProbability := float64(c.capacity-size) / float64(c.capacity)
	cachePressure := float64(currentSize) / float64(c.capacity)
	threshold := c.admitThreshold * (1 - cachePressure)

	return rand.Float64() <= baseProbability && baseProbability > threshold
}

func (c *SieveCache[K, V]) cleanupGhostCache() {
	var oldestKey K
	var oldestTime time.Time

	for k, v := range c.ghostCache {
		if oldestTime.IsZero() || v.evicted.Before(oldestTime) {
			oldestKey = k
			oldestTime = v.evicted
		}
	}

	delete(c.ghostCache, oldestKey)
}

// GetStats returns current cache statistics and performance metrics.
func (c *SieveCache[K, V]) GetStats() Stats {
	c.mu.RLock()
	defer c.mu.RUnlock()

	hitCount := c.hitCount.Load()
	missCount := c.missCount.Load()
	total := hitCount + missCount
	hitRate := float64(0)
	if total > 0 {
		hitRate = float64(hitCount) / float64(total)
	}

	uptime := time.Since(c.startTime)
	opsPerSecond := float64(total) / uptime.Seconds()

	return Stats{
		Hits:          hitCount,
		Misses:        missCount,
		HitRate:       hitRate,
		CurrentSize:   c.currentSize.Load(),
		Capacity:      c.capacity,
		ItemCount:     len(c.items),
		GhostCount:    len(c.ghostCache),
		EvictionCount: c.evictionCount.Load(),
		Uptime:        uptime,
		OpsPerSecond:  opsPerSecond,
	}
}

// Clear removes all entries from the cache and resets statistics.
func (c *SieveCache[K, V]) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.items = make(map[K]*entry[K, V])
	c.ghostCache = make(map[K]*ghostEntry)
	c.currentSize.Store(0)
	c.hitCount.Store(0)
	c.missCount.Store(0)
	c.evictionCount.Store(0)
}
