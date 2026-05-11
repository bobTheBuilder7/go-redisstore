// Package redisstore defines a redis-backed storage system for limiting.
package redisstore

import (
	"context"
	"fmt"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/sethvargo/go-limiter"
)

const (
	// hash field keys shared by the Lua script.
	fieldInterval  = "i"
	fieldMaxTokens = "m"
	fieldTokens    = "k"

	weekSeconds = 60 * 60 * 24 * 7
)

var _ limiter.Store = (*Store)(nil)

type Store struct {
	tokens    uint64
	interval  time.Duration
	pool      *redis.Client
	luaScript *redis.Script
	stopped   uint32
}

// Config is used as input to New. It defines the behavior of the storage system.
type Config struct {
	// Tokens is the number of tokens to allow per interval. The default value is 1.
	Tokens uint64

	// Interval is the time interval upon which to enforce rate limiting. The default value is 1 second.
	Interval time.Duration
}

// New creates a new limiter backed by the given Redis client.
func New(c *Config, pool *redis.Client) (*Store, error) {
	if c == nil {
		c = new(Config)
	}

	tokens := uint64(1)
	if c.Tokens > 0 {
		tokens = c.Tokens
	}

	interval := 1 * time.Second
	if c.Interval > 0 {
		interval = c.Interval
	}

	return &Store{
		tokens:    tokens,
		interval:  interval,
		pool:      pool,
		luaScript: redis.NewScript(luaTemplate),
	}, nil
}

// Take attempts to remove a token from the named key. If the take is
// successful, it returns true, otherwise false. It also returns the configured
// limit, remaining tokens, and reset time.
func (s *Store) Take(ctx context.Context, key string) (limit, remaining, next uint64, ok bool, retErr error) {
	if atomic.LoadUint32(&s.stopped) == 1 {
		retErr = limiter.ErrStopped
		return
	}

	now := uint64(time.Now().UTC().UnixNano())
	res, err := s.luaScript.Run(ctx, s.pool, []string{key},
		strconv.FormatUint(now, 10),
		strconv.FormatUint(s.tokens, 10),
		strconv.FormatInt(s.interval.Nanoseconds(), 10),
	).Slice()
	if err != nil {
		retErr = fmt.Errorf("failed to run script: %w", err)
		return
	}

	if len(res) < 4 {
		retErr = fmt.Errorf("unexpected response length %d", len(res))
		return
	}

	limit = uint64(res[0].(int64))
	remaining = uint64(res[1].(int64))
	next = uint64(res[2].(int64))
	// Lua true → RESP integer 1; Lua false → RESP null (nil)
	ok = res[3] != nil
	return
}

// Get gets the current limit and remaining tokens for the key without
// consuming any tokens.
func (s *Store) Get(ctx context.Context, key string) (limit, remaining uint64, retErr error) {
	if atomic.LoadUint32(&s.stopped) == 1 {
		retErr = limiter.ErrStopped
		return
	}

	vals, err := s.pool.HMGet(ctx, key, fieldMaxTokens, fieldTokens).Result()
	if err != nil {
		retErr = fmt.Errorf("failed to get key: %w", err)
		return
	}

	if len(vals) < 2 {
		retErr = fmt.Errorf("not enough fields returned: expected 2 got %d", len(vals))
		return
	}

	if vals[0] != nil {
		v, err := strconv.ParseUint(vals[0].(string), 10, 64)
		if err != nil {
			retErr = fmt.Errorf("failed to parse limit: %w", err)
			return
		}
		limit = v
	}

	if vals[1] != nil {
		v, err := strconv.ParseUint(vals[1].(string), 10, 64)
		if err != nil {
			retErr = fmt.Errorf("failed to parse remaining: %w", err)
			return
		}
		remaining = v
	}

	return
}

// Set sets the key's limit to the provided value and interval.
func (s *Store) Set(ctx context.Context, key string, tokens uint64, interval time.Duration) (retErr error) {
	if atomic.LoadUint32(&s.stopped) == 1 {
		retErr = limiter.ErrStopped
		return
	}

	tokensStr := strconv.FormatUint(tokens, 10)
	intervalStr := strconv.FormatInt(interval.Nanoseconds(), 10)

	pipe := s.pool.Pipeline()
	pipe.HSet(ctx, key,
		fieldTokens, tokensStr,
		fieldMaxTokens, tokensStr,
		fieldInterval, intervalStr,
	)
	pipe.Expire(ctx, key, time.Duration(weekSeconds)*time.Second)

	if _, err := pipe.Exec(ctx); err != nil {
		retErr = fmt.Errorf("failed to set key: %w", err)
	}
	return
}

// Burst adds the given tokens to the key's current token count.
func (s *Store) Burst(ctx context.Context, key string, tokens uint64) (retErr error) {
	if atomic.LoadUint32(&s.stopped) == 1 {
		retErr = limiter.ErrStopped
		return
	}

	pipe := s.pool.Pipeline()
	pipe.HIncrBy(ctx, key, fieldTokens, int64(tokens))
	pipe.Expire(ctx, key, time.Duration(weekSeconds)*time.Second)

	if _, err := pipe.Exec(ctx); err != nil {
		retErr = fmt.Errorf("failed to burst key: %w", err)
	}

	return
}

// Close stops the store and releases any open connections.
func (s *Store) Close(_ context.Context) error {
	if !atomic.CompareAndSwapUint32(&s.stopped, 0, 1) {
		return nil
	}

	if err := s.pool.Close(); err != nil {
		return fmt.Errorf("failed to close pool: %w", err)
	}

	return nil
}
