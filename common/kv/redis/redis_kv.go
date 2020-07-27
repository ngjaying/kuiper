package redis

import (
	"fmt"
	"github.com/emqx/kuiper/common"
)

// save key/value to the namespace
type RedisKVStore struct {
	client    Client // a singleton
	namespace string
}

func NewRedisKVStore(config RedisConf, ns string) (*RedisKVStore, error) {
	client, err := NewClient(config)
	if err != nil {
		return nil, err
	}
	r := &RedisKVStore{
		client:    client,
		namespace: ns,
	}
	return r, nil
}

func (s *RedisKVStore) Open() error {
	// do nothing
	return nil
}

func (s *RedisKVStore) Close() error {
	// do nothing
	return nil
}

func (s *RedisKVStore) Set(key string, value interface{}) error {
	switch v := value.(type) {
	case string, []byte:
		conn := s.client.GetConnection()
		defer conn.Close()
		_ = conn.Send("MULTI")
		_ = conn.Send("SADD", fmt.Sprintf("%s:__myset", s.namespace), key)
		_ = conn.Send("SETNX", fmt.Sprintf("%s:%s", s.namespace, key), v)
		n, err := conn.Do("EXEC")
		if err != nil {
			return err
		}
		if n == 0 {
			return fmt.Errorf("key %s already exists", key)
		}
	default:
		return fmt.Errorf("invalid data type of value %v, only support string or []byte", value)
	}
	return nil
}

func (s *RedisKVStore) Replace(key string, value interface{}) error {
	switch v := value.(type) {
	case string, []byte:
		conn := s.client.GetConnection()
		defer conn.Close()
		_ = conn.Send("MULTI")
		_ = conn.Send("SADD", fmt.Sprintf("%s:__myset", s.namespace), key)
		_ = conn.Send("SET", fmt.Sprintf("%s:%s", s.namespace, key), v)
		_, err := conn.Do("EXEC")
		if err != nil {
			return err
		}
	default:
		return fmt.Errorf("invalid data type of value %v, only support string or []byte", value)
	}
	return nil
}

func (s *RedisKVStore) Get(key string) (interface{}, bool) {
	conn := s.client.GetConnection()
	defer conn.Close()
	v, err := conn.Do("GET", fmt.Sprintf("%s:%s", s.namespace, key))
	if err != nil {
		common.Log.Errorf("%s", err)
		return nil, false
	}
	return v, true
}

func (s *RedisKVStore) Delete(key string) error {
	conn := s.client.GetConnection()
	defer conn.Close()
	_ = conn.Send("MULTI")
	_ = conn.Send("SREM", fmt.Sprintf("%s:__myset", s.namespace))
	_ = conn.Send("DEL", fmt.Sprintf("%s:%s", s.namespace, key))
	_, err := conn.Do("EXEC")
	if err != nil {
		return err
	}
	return nil
}

func (s *RedisKVStore) Keys() ([]string, error) {
	conn := s.client.GetConnection()
	defer conn.Close()
	v, err := conn.Do("SMEMBERS", fmt.Sprintf("%s:__myset", s.namespace))
	if err != nil {
		return nil, err
	}
	if arr, ok := v.([]interface{}); ok {
		keys := make([]string, len(arr))
		for i, a := range arr {
			keys[i] = fmt.Sprintf("%v", a)
		}
		return keys, nil
	} else {
		return nil, fmt.Errorf("Invalid return result %v from redis, should be an array", v)
	}
}

func MockRedisKVStore(client Client, ns string) (*RedisKVStore, error) {
	return &RedisKVStore{
		client:    client,
		namespace: ns,
	}, nil
}
