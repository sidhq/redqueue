package requeue

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

// Queue provides a simple method for creating distributed mutexes using multiple Redis connection pools.
type Queue struct {
	name   string
	client *redis.Client
	expiry time.Duration
}

// New creates and returns a new Redsync instance from given Redis connection pools.
func New(client *redis.Client, name string) *Queue {
	return &Queue{
		name:   name,
		client: client,
		expiry: 8 * time.Second,
	}
}

var pushScript = redis.NewScript(`
	if redis.call("EXISTS", KEYS[2]) == 0 then
		return redis.call("LPUSH", KEYS[1], KEYS[2])
	else
		return 0
	end
`)

var popScript = redis.NewScript(`
	val = redis.call("RPOP", KEYS[1])
	if val ~= false then
		redis.call("SET", val, ARGV[1], "PX", ARGV[2])
		return val
	else
		return 0
	end
`)

func (r *Queue) Push(ctx context.Context, value string) (bool, error) {
	ok, err := pushScript.Run(ctx, r.client, []string{r.name, value}).Result()
	if err != nil {
		return false, err
	}
	if ok.(int64) == 0 {
		return false, nil
	}
	return true, nil
}

func (r *Queue) Pop(ctx context.Context) (*Lease, error) {
	value, err := genValue()
	if err != nil {
		return nil, err
	}
	key, err := popScript.Run(ctx, r.client, []string{r.name}, value, int(r.expiry/time.Millisecond)).Result()
	if err != nil {
		return nil, err
	}
	switch key := key.(type) {
	case string:
		return r.NewLease(key, time.Now().Add(r.expiry), value), nil
	case int64:
		return nil, nil
	default:
		return nil, ErrFailed
	}
}

// NewLease returns a new distributed mutex with given name.
func (r *Queue) NewLease(name string, until time.Time, value string) *Lease {
	m := &Lease{
		name:   name,
		expiry: 8 * time.Second,
		client: r.client,
	}
	return m
}

// An Option configures a mutex.
type Option interface {
	Apply(*Lease)
}

// OptionFunc is a function that configures a mutex.
type OptionFunc func(*Lease)

// Apply calls f(mutex)
func (f OptionFunc) Apply(mutex *Lease) {
	f(mutex)
}
