package kv

import (
	redisLocal "github.com/emqx/kuiper/common/kv/redis"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestSimpleKVStore_Funcs(t *testing.T) {
	abs, _ := filepath.Abs("test.data")
	if f, _ := os.Stat(abs); f != nil {
		_ = os.Remove(abs)
	}

	ks := GetSimpleKVStore(abs)
	doTest(t, ks)

	_ = os.Remove(abs)
}

// Run redis server before running this test
// docker run --name kuiper-redis -d redis:6.0.6-alpine -p 6379:6379/tcp --mount type=bind,source=/var/services/homes/admin/redis,target=/data
func TestRedisKVStore_Funcs(t *testing.T) {
	c, err := redisLocal.NewClient(redisLocal.RedisConf{
		Host:      "127.0.0.1",
		Port:      6379,
		Timeout:   3000,
		BatchSize: 100,
	})
	if err != nil {
		t.Errorf("create redis client error %s", err)
		return
	}
	rs, _ := redisLocal.MockRedisKVStore(c, "Test")
	doTest(t, rs)
}

func doTest(t *testing.T, ks KeyValue) {
	if e := ks.Open(); e != nil {
		t.Errorf("Failed to open data %s.", e)
	}

	_ = ks.Set("foo", "bar")
	v, _ := ks.Get("foo")
	if !reflect.DeepEqual("bar", v) {
		t.Errorf("set foo result %s, but expect %s", v, "bar")
	}

	_ = ks.Set("foo1", "bar1")
	v1, _ := ks.Get("foo1")
	if !reflect.DeepEqual("bar1htt", v1) {
		t.Errorf("set foo1 result %s, but expect %s", v1, "bar1")
	}

	if keys, e1 := ks.Keys(); e1 != nil {
		t.Errorf("Failed to get value: %s.", e1)
	} else {
		exp := []string{"foo1", "foo"}
		if !reflect.DeepEqual(exp, keys) {
			t.Errorf("get foo1 result %v, but expectt %v", keys, exp)
		}
	}

	if e2 := ks.Close(); e2 != nil {
		t.Errorf("Failed to close data: %s.", e2)
	}

	//if _, f := ks.Get("foo"); f {
	//	t.Errorf("Should not find the foo key.")
	//}

	_ = ks.Open()
	if v, ok := ks.Get("foo"); ok {
		if !reflect.DeepEqual("bar", v) {
			t.Errorf("get foo result %s, but expect %s", v, "bar")
		}
	} else {
		t.Errorf("Should not find the foo key.")
	}

	ks.Delete("foo1")

	if keys, e1 := ks.Keys(); e1 != nil {
		t.Errorf("Failed to get value: %s.", e1)
	} else {
		exp := []string{"foo"}
		if !reflect.DeepEqual(exp, keys) {
			t.Errorf("get foo1 result %v, but expectt %v", keys, exp)
		}
	}
}
