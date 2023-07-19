package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/sidhq/redqueue"
)

func main() {

	client := redis.NewClient(&redis.Options{Addr: "localhost:6379", Password: "", DB: 0})
	queue := redqueue.New(client, "queue")

	go func() {
		for {
			for i := 0; i < 100; i++ {
				ok, err := queue.Push(context.Background(), fmt.Sprintf("%d", i), time.Second)
				if err != nil {
					panic(err)
				}
				if !ok {
					log.Printf("push failed")
					time.Sleep(100 * time.Millisecond)
					continue
				} else {
					log.Printf("push ok")
				}
			}
		}
	}()
	for i := 0; i < 3; i++ {
		go func(id int) {
			for {
				lease, err := queue.Pop(context.Background())
				if err != nil {
					panic(err)
				}
				if lease == nil {
					continue
				}
				time.Sleep(500 * time.Millisecond)
				if ok, err := lease.Extend(context.Background()); err != nil {
					panic(err)
				} else if !ok {
					log.Printf("extend failed")
				}
				time.Sleep(600 * time.Millisecond)
				ok, err := lease.Unlock(context.Background())
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
