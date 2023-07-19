package main

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/sidhq/redqueue"
)

func main() {

	client := redis.NewClient(&redis.Options{Addr: "localhost:6379", Password: "", DB: 0})
	queue := redqueue.New(client, "test")

	keys := make([]bool, 100)
	go func() {
		for {
			for i := 0; i < 100; i++ {
				ok, err := queue.Push(context.Background(), fmt.Sprintf("%d", i), time.Second)
				if err != nil {
					panic(err)
				}
				if !ok {
					time.Sleep(100 * time.Millisecond)
					continue
				}
			}
			time.Sleep(100 * time.Millisecond)
		}
	}()
	for i := 0; i < 20; i++ {
		go func(id int) {
			for {
				lease, err := queue.Pop(context.Background())
				if err != nil {
					panic(err)
				}
				if lease == nil {
					continue
				}
				idx, _ := strconv.Atoi(lease.Item())
				keys[idx] = true
				time.Sleep(500 * time.Millisecond)
				if ok, err := lease.Extend(context.Background()); err != nil {
					panic(err)
				} else if !ok {
					log.Printf("extend failed")
				}
				time.Sleep(600 * time.Millisecond)
				keys[idx] = false
				ok, err := lease.Release(context.Background())
				if err != nil {
					panic(err)
				}
				if !ok {
					log.Printf("unlock failed")
				}
			}
		}(i)
	}
	for {
		time.Sleep(1 * time.Second)
	}
}
