package redisstore_test

import (
	"context"
	"log"
	"time"

	redisstore "github.com/bobTheBuilder7/go-redisstore"
	"github.com/redis/go-redis/v9"
)

func ExampleNew() {
	ctx := context.Background()

	client := redis.NewClient(&redis.Options{
		Addr: "127.0.0.1:6379",
	})
	defer client.Close()

	store, err := redisstore.New(&redisstore.Config{
		Tokens:   15,
		Interval: time.Minute,
	}, client)
	if err != nil {
		log.Fatal(err)
	}
	defer store.Close(ctx)

	limit, remaining, reset, ok, err := store.Take(ctx, "my-key")
	if err != nil {
		log.Fatal(err)
	}
	_, _, _, _ = limit, remaining, reset, ok
}
