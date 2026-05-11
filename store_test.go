package redisstore

import (
	"crypto/rand"
	"crypto/sha256"
	"fmt"
	"io"
	"log"
	"sort"
	"testing"
	"time"

	"github.com/bobTheBuilder7/assert"
	"github.com/redis/go-redis/v9"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

func newRedisClient(t *testing.T) *redis.Client {
	t.Helper()

	redisC, err := testcontainers.Run(
		t.Context(), "redis:latest",
		testcontainers.WithLogger(log.New(io.Discard, "", 0)),
		testcontainers.WithExposedPorts("6379/tcp"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("Ready to accept connections"),
			wait.ForListeningPort("6379/tcp"),
		),
	)
	defer testcontainers.CleanupContainer(t, redisC)
	assert.Nil(t, err)

	endpoint, err := redisC.PortEndpoint(t.Context(), "6379/tcp", "")
	assert.Nil(t, err)

	client := redis.NewClient(&redis.Options{Addr: endpoint})
	t.Cleanup(func() { client.Close() })

	err = client.Ping(t.Context()).Err()
	assert.Nil(t, err)

	return client
}

func testKey(tb testing.TB) string {
	tb.Helper()

	var b [512]byte
	if _, err := rand.Read(b[:]); err != nil {
		tb.Fatalf("failed to generate random string: %v", err)
	}
	digest := fmt.Sprintf("%x", sha256.Sum256(b[:]))
	return digest[:32]
}

func TestStore_Exercise(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	client := newRedisClient(t)

	s, err := New(&Config{
		Tokens:   5,
		Interval: 3 * time.Second,
	}, client)
	assert.Nil(t, err)
	t.Cleanup(func() { s.Close(ctx) })

	key := testKey(t)

	// Get when no config exists
	{
		limit, remaining, err := s.Get(ctx, key)
		assert.Nil(t, err)
		assert.Equal(t, limit, uint64(0))
		assert.Equal(t, remaining, uint64(0))
	}

	// Take with no key configuration — uses default values
	{
		limit, remaining, reset, ok, err := s.Take(ctx, key)
		assert.Nil(t, err)
		assert.True(t, ok)
		assert.Equal(t, limit, uint64(5))
		assert.Equal(t, remaining, uint64(4))
		assert.True(t, time.Until(time.Unix(0, int64(reset))) <= 3*time.Second)
	}

	// Get the value
	{
		limit, remaining, err := s.Get(ctx, key)
		assert.Nil(t, err)
		assert.Equal(t, limit, uint64(5))
		assert.Equal(t, remaining, uint64(4))
	}

	// Set a new configuration
	{
		err := s.Set(ctx, key, 11, 5*time.Second)
		assert.Nil(t, err)
	}

	// Get after Set
	{
		limit, remaining, err := s.Get(ctx, key)
		assert.Nil(t, err)
		assert.Equal(t, limit, uint64(11))
		assert.Equal(t, remaining, uint64(11))
	}

	// Take uses new configuration
	{
		limit, remaining, reset, ok, err := s.Take(ctx, key)
		assert.Nil(t, err)
		assert.True(t, ok)
		assert.Equal(t, limit, uint64(11))
		assert.Equal(t, remaining, uint64(10))
		assert.True(t, time.Until(time.Unix(0, int64(reset))) <= 5*time.Second)
	}

	// Get after Take
	{
		limit, remaining, err := s.Get(ctx, key)
		assert.Nil(t, err)
		assert.Equal(t, limit, uint64(11))
		assert.Equal(t, remaining, uint64(10))
	}

	// Burst then Take
	{
		err := s.Burst(ctx, key, 5)
		assert.Nil(t, err)

		limit, remaining, reset, ok, err := s.Take(ctx, key)
		assert.Nil(t, err)
		assert.True(t, ok)
		assert.Equal(t, limit, uint64(11))
		assert.Equal(t, remaining, uint64(14))
		assert.True(t, time.Until(time.Unix(0, int64(reset))) <= 5*time.Second)
	}

	// Final Get
	{
		limit, remaining, err := s.Get(ctx, key)
		assert.Nil(t, err)
		assert.Equal(t, limit, uint64(11))
		assert.Equal(t, remaining, uint64(14))
	}
}

func TestStore_Take(t *testing.T) {
	t.Parallel()

	if testing.Short() {
		t.Skipf("skipping (short)")
	}

	ctx := t.Context()
	client := newRedisClient(t)

	cases := []struct {
		name     string
		tokens   uint64
		interval time.Duration
	}{
		{
			name:     "second",
			tokens:   10,
			interval: 1 * time.Second,
		},
	}

	for _, tc := range cases {
		tc := tc

		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			key := testKey(t)

			s, err := New(&Config{
				Interval: tc.interval,
				Tokens:   tc.tokens,
			}, client)
			assert.Nil(t, err)
			t.Cleanup(func() { s.Close(ctx) })

			type result struct {
				limit, remaining uint64
				reset            time.Duration
				ok               bool
				err              error
			}

			// Take twice the token count concurrently
			takeCh := make(chan *result, 2*tc.tokens)
			for i := uint64(1); i <= 2*tc.tokens; i++ {
				go func() {
					limit, remaining, reset, ok, err := s.Take(ctx, key)
					takeCh <- &result{limit, remaining, time.Until(time.Unix(0, int64(reset))), ok, err}
				}()
			}

			var results []*result
			for i := uint64(1); i <= 2*tc.tokens; i++ {
				select {
				case r := <-takeCh:
					results = append(results, r)
				case <-time.After(5 * time.Second):
					t.Fatal("timeout waiting for Take")
				}
			}

			sort.Slice(results, func(i, j int) bool {
				if results[i].remaining == results[j].remaining {
					return !results[j].ok
				}
				return results[i].remaining > results[j].remaining
			})

			for i, r := range results {
				assert.Nil(t, r.err)
				assert.Equal(t, r.limit, tc.tokens)
				assert.True(t, r.reset <= tc.interval)

				if uint64(i) < tc.tokens {
					assert.Equal(t, r.remaining, tc.tokens-uint64(i)-1)
					assert.True(t, r.ok)
				} else {
					assert.Equal(t, r.remaining, uint64(0))
					assert.False(t, r.ok)
				}
			}

			// Wait for the interval to reset
			time.Sleep(tc.interval)

			_, _, _, ok, err := s.Take(ctx, key)
			assert.Nil(t, err)
			assert.True(t, ok)
		})
	}
}
