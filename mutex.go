package redqueue

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"time"

	"github.com/redis/go-redis/v9"
)

var ErrFailed = errors.New("redsync: failed to acquire lock")
var ErrExtendFailed = errors.New("redsync: failed to extend lock")

// A Lease is a distributed mutual exclusion lock.
type Lease struct {
	item   string
	key    string
	expiry time.Duration

	driftFactor float64

	token string
	until time.Time

	client *redis.Client
}

// Item returns mutex name (i.e. the Redis key).
func (m *Lease) Item() string {
	return m.item
}

// Until returns the time of validity of acquired lock. The value will be zero value until a lock is acquired.
func (m *Lease) Until() time.Time {
	return m.until
}

// Release unlocks m and returns the status of unlock.
func (m *Lease) Release(ctx context.Context) (bool, error) {
	return m.release(ctx, m.client, m.token)
}

// Extend resets the mutex's expiry and returns the status of expiry extension.
func (m *Lease) Extend(ctx context.Context) (bool, error) {
	start := time.Now()
	held, err := m.touch(ctx, m.client, m.token, m.expiry)
	if err != nil {
		return false, err
	}
	if !held {
		return false, nil
	}
	now := time.Now()
	until := now.Add(m.expiry - now.Sub(start) - time.Duration(int64(float64(m.expiry)*m.driftFactor)))
	if now.Before(until) {
		m.until = until
		return true, nil
	}
	return false, ErrExtendFailed
}

func genToken() (string, error) {
	b := make([]byte, 16)
	_, err := rand.Read(b)
	if err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(b), nil
}

var deleteScript = redis.NewScript(`
	if redis.call("GET", KEYS[1]) == ARGV[1] then
		return redis.call("DEL", KEYS[1])
	else
		return 0
	end
`)

func (m *Lease) release(ctx context.Context, client *redis.Client, token string) (bool, error) {
	status, err := deleteScript.Run(ctx, client, []string{m.key}, token).Result()
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

func (m *Lease) touch(ctx context.Context, client *redis.Client, token string, expiry time.Duration) (bool, error) {
	status, err := touchScript.Run(ctx, client, []string{m.key}, token, int(expiry/time.Millisecond)).Result()
	if err != nil {
		return false, err
	}
	return status != int64(0), nil
}
