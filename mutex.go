package requeue

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"time"

	"github.com/redis/go-redis/v9"
)

// A DelayFunc is used to decide the amount of time to wait between retries.
type DelayFunc func(tries int) time.Duration

// A Lease is a distributed mutual exclusion lock.
type Lease struct {
	name   string
	expiry time.Duration

	driftFactor float64

	value string
	until time.Time

	client *redis.Client
}

// Name returns mutex name (i.e. the Redis key).
func (m *Lease) Name() string {
	return m.name
}

// Until returns the time of validity of acquired lock. The value will be zero value until a lock is acquired.
func (m *Lease) Until() time.Time {
	return m.until
}

// Unlock unlocks m and returns the status of unlock.
func (m *Lease) Unlock(ctx context.Context) (bool, error) {
	return m.release(ctx, m.client, m.value)
}

// Extend resets the mutex's expiry and returns the status of expiry extension.
func (m *Lease) Extend(ctx context.Context) (bool, error) {
	start := time.Now()
	held, err := m.touch(ctx, m.client, m.value, int(m.expiry/time.Millisecond))
	if err != nil {
		return false, err
	}
	if !held {
		return false, ErrFailed
	}
	now := time.Now()
	until := now.Add(m.expiry - now.Sub(start) - time.Duration(int64(float64(m.expiry)*m.driftFactor)))
	if now.Before(until) {
		m.until = until
		return true, nil
	}
	return false, ErrExtendFailed
}

func genValue() (string, error) {
	b := make([]byte, 16)
	_, err := rand.Read(b)
	if err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(b), nil
}

func (m *Lease) acquire(ctx context.Context, client *redis.Client, value string) (bool, error) {
	reply, err := client.SetNX(ctx, m.name, value, m.expiry).Result()
	if err != nil {
		return false, err
	}
	return reply, nil
}

var deleteScript = redis.NewScript(`
	if redis.call("GET", KEYS[1]) == ARGV[1] then
		return redis.call("DEL", KEYS[1])
	else
		return 0
	end
`)

func (m *Lease) release(ctx context.Context, client *redis.Client, value string) (bool, error) {
	status, err := deleteScript.Run(ctx, client, []string{m.name}, value).Result()
	if err != nil {
		return false, err
	}
	return status != int64(0), nil
}

var touchScript = redis.NewScript(`
	if redis.call("GET", KEYS[1]) == ARGV[1] then
		return redis.call("PEXPIRE", KEYS[1], ARGV[2])
	else
		return 0
	end
`)

func (m *Lease) touch(ctx context.Context, client *redis.Client, value string, expiry int) (bool, error) {
	status, err := touchScript.Run(ctx, client, []string{m.name}, value, expiry).Result()
	if err != nil {
		return false, err
	}
	return status != int64(0), nil
}
