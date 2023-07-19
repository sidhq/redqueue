package main

import (
	"context"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
	requeue "github.com/sidhq/redis-queue"
)

func main() {

	client := redis.NewClient(&redis.Options{Addr: "localhost:6379", Password: "", DB: 0})
	queue := requeue.New(client, "queue")

	val := make([]int, 1)

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			mutex := queue.NewLease("test")
			for {
				ok, err := mutex.Lock(context.Background())
				if err != nil {
					panic(err)
				}
				if !ok {
					time.Sleep(100 * time.Millisecond)
					continue
				}
				defer mutex.Unlock(context.Background())
				val[0]++
				return
			}
		}()
	}
	wg.Wait()
	println(val[0])
}
