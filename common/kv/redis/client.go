package redis

import (
	"fmt"
	"github.com/gomodule/redigo/redis"
	"sync"
	"time"
)

var currClient *RedisClient // a singleton so Readings can be de-referenced
var once sync.Once

type Client interface {
	GetConnection() redis.Conn
}

// Client represents a Redis client
type RedisClient struct {
	Pool      *redis.Pool // A thread-safe pool of connections to Redis
	BatchSize int
}

type RedisConf struct {
	Host      string
	Port      int
	Timeout   int
	Password  string
	BatchSize int
}

// Return a pointer to the Redis client
func NewClient(config RedisConf) (*RedisClient, error) {
	once.Do(func() {
		connectionString := fmt.Sprintf("%s:%d", config.Host, config.Port)
		opts := []redis.DialOption{
			redis.DialConnectTimeout(time.Duration(config.Timeout) * time.Millisecond),
		}
		if config.Password != "" {
			opts = append(opts, redis.DialPassword(config.Password))
		}

		dialFunc := func() (redis.Conn, error) {
			conn, err := redis.Dial(
				"tcp", connectionString, opts...,
			)
			if err != nil {
				return nil, fmt.Errorf("Could not dial Redis: %s", err)
			}
			return conn, nil
		}
		// Default the batch size to 1,000 if not set
		batchSize := 1000
		if config.BatchSize != 0 {
			batchSize = config.BatchSize
		}
		currClient = &RedisClient{
			Pool: &redis.Pool{
				IdleTimeout: 0,
				MaxIdle:     10,
				Dial:        dialFunc,
			},
			BatchSize: batchSize,
		}
	})

	// Test connectivity now so don't have failures later when doing lazy connect.
	if _, err := currClient.Pool.Dial(); err != nil {
		return nil, err
	}

	return currClient, nil
}

func (c *RedisClient) GetConnection() redis.Conn {
	return c.Pool.Get()
}
