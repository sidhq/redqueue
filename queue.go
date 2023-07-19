package redqueue

import (
	"context"
	"time"

	"github.com/pkg/errors"
	"github.com/redis/go-redis/v9"
)

// Queue provides a simple method for creating distributed mutexes using multiple Redis connection pools.
type Queue struct {
	name        string
	client      *redis.Client
	expiry      time.Duration
	driftFactor float64
}

// New creates and returns a new Redsync instance from given Redis connection pools.
func New(client *redis.Client, name string) *Queue {
	return &Queue{
		name:        name + ":requeue",
		client:      client,
		expiry:      8 * time.Second,
		driftFactor: 0.05,
	}
}

var pushScript = redis.NewScript(`
	if redis.call("EXISTS", KEYS[2]) == 0 then
		redis.call("LPUSH", KEYS[1], KEYS[2])
		redis.call("SET", KEYS[2], 1, "PX", ARGV[1])
		return 1
	else
		return 0
	end
`)

func (r *Queue) Push(ctx context.Context, name string, expiry time.Duration) (bool, error) {
	ok, err := pushScript.Run(ctx, r.client, []string{r.name, name}, int(expiry/time.Millisecond)).Result()
	if err != nil {
		return false, err
	}
	if ok.(int64) == 0 {
		return false, nil
	}
	return true, nil
}

func (r *Queue) Pop(ctx context.Context) (*Lease, error) {
	start := time.Now()
	value, err := genValue()
	if err != nil {
		return nil, errors.Wrap(err, "generate value")
	}
	key, err := r.client.RPop(ctx, r.name).Result()
	if err == redis.Nil {
		return nil, nil
	}
	if err != nil {
		return nil, errors.Wrap(err, "pop")
	}
	ok, err := r.client.SetXX(ctx, key, value, r.expiry).Result()
	if err != nil {
		return nil, errors.Wrap(err, "set expiry")
	}
	if !ok {
		return nil, nil
	}

	now := time.Now()
	until := now.Add(r.expiry - now.Sub(start) - time.Duration(int64(float64(r.expiry)*r.driftFactor)))
	return r.newLease(key, until, value), nil
}

// newLease returns a new distributed mutex with given name.
func (r *Queue) newLease(name string, until time.Time, value string) *Lease {
	m := &Lease{
		name:        name,
		expiry:      1 * time.Second,
		client:      r.client,
		driftFactor: r.driftFactor,
		value:       value,
		until:       until,
	}
	return m
}
