package memcache

import (
	"fmt"
	"path"
	"strconv"
	"time"

	"github.com/bradfitz/gomemcache/memcache"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/kelub/goose/pkg/prom"
)

var (
	memProm = prom.NewPromVec("memcached").
		Counter("counter", "Counter", []string{"name", "status"}).
		Histogram("duration_seconds",
			"Bucketed histogram of memcached response time duration",
			[]string{"name", "status"},
			prometheus.ExponentialBuckets(0.001, 2.0, 15))
)

type MemcacheClient interface {
	Get(key string) (bool, []byte, error)
	GetMulti(keys []string) (map[string][]byte, error)
	Remove(key string) (bool, error)
	Set(key string, value []byte, ttl time.Duration) error
	Increment(key string, delta int64) (uint64, error)
	TryLockOnce(key string) (bool, error)
	Unlock(key string) (bool, error)

	GetWithFlags(key string) (bool, []byte, uint32, error)
	GetMultiWithFlags(keys []string) (map[string][]byte, map[string]uint32, error)
	SetWithFlags(key string, value []byte, ttl time.Duration, flags uint32) error
}

type MemcacheOption struct {
	Host         []string
	Prefix       string
	VersionFunc  *func() string
	MaxIdleConns int
}

func NewMemcacheClient(opt MemcacheOption) (*Client, error) {
	if len(opt.Host) == 0 {
		return nil, fmt.Errorf("no memcache host")
	}

	var keyFunc func(string) string
	if opt.VersionFunc == nil {
		keyFunc = func(key string) string {
			return path.Join(opt.Prefix, key)
		}
	} else {
		keyFunc = func(key string) string {
			return path.Join(opt.Prefix, (*opt.VersionFunc)(), key)
		}
	}

	client := memcache.New(opt.Host...)
	client.Timeout = time.Second
	if opt.MaxIdleConns <= 0 {
		opt.MaxIdleConns = 10
	}
	client.MaxIdleConns = opt.MaxIdleConns

	return &Client{
		KeyFunc: keyFunc,
		client:  client,
	}, nil
}

type Client struct {
	client *memcache.Client

	KeyFunc func(string) string
}

func (m *Client) Get(key string) (bool, []byte, error) {
	start := time.Now()
	item, err := m.client.Get(m.KeyFunc(key))

	if err == memcache.ErrCacheMiss {
		memProm.Inc("get", "miss")
		memProm.HandleTime(start, "get", "miss")
		return false, nil, nil
	} else if err != nil {
		memProm.Inc("get", "failed")
		memProm.HandleTime(start, "get", "failed")
		return false, nil, err
	}
	memProm.Inc("get", "hit")
	memProm.HandleTime(start, "get", "hit")
	return true, item.Value, nil
}

func (m *Client) GetMulti(keys []string) (map[string][]byte, error) {
	start := time.Now()
	var newkey string
	keysWithPrefix := make([]string, len(keys))
	keyMap := make(map[string]string, len(keys))
	for i, k := range keys {
		newkey = m.KeyFunc(k)
		keysWithPrefix[i] = newkey
		keyMap[newkey] = k
	}

	ret := make(map[string][]byte)
	itemMap, err := m.client.GetMulti(keysWithPrefix)
	if err != nil {
		memProm.Inc("get_multi", "failed")
		memProm.HandleTime(start, "get_multi", "failed")
		return ret, err
	}

	for k, v := range itemMap {
		ret[keyMap[k]] = v.Value
	}
	memProm.Inc("get_multi", "hit")
	memProm.HandleTime(start, "get_multi", "hit")
	return ret, nil
}

func (m *Client) Remove(key string) (bool, error) {
	start := time.Now()
	err := m.client.Delete(m.KeyFunc(key))

	if err == nil {
		memProm.Inc("remove", "hit")
		memProm.HandleTime(start, "remove", "hit")
		return true, nil
	}
	if err == memcache.ErrCacheMiss {
		memProm.Inc("remove", "miss")
		memProm.HandleTime(start, "remove", "miss")
		return false, nil
	}
	memProm.Inc("remove", "failed")
	memProm.HandleTime(start, "remove", "failed")
	return false, err
}

func (m *Client) Set(key string, value []byte, ttl time.Duration) error {
	start := time.Now()
	if ttl > 30*24*time.Hour {
		ttl = 30 * 24 * time.Hour
	}
	item := &memcache.Item{
		Key:        m.KeyFunc(key),
		Value:      value,
		Expiration: int32(ttl.Seconds()),
	}
	if err := m.client.Set(item); err != nil {
		memProm.Inc("set", "failed")
		memProm.HandleTime(start, "set", "failed")
		return err
	}
	memProm.Inc("set", "hit")
	memProm.HandleTime(start, "set", "hit")
	return nil
}

func (m *Client) Increment(key string, delta int64) (uint64, error) {
	var f func(string, uint64) (uint64, error)
	if delta > 0 {
		f = m.client.Increment
	} else {
		f = m.client.Decrement
		delta *= -1
	}

	realkey := m.KeyFunc(key)

	newv, err := f(realkey, uint64(delta))
	if err == nil || err != memcache.ErrCacheMiss {
		return newv, err
	}

	//try to add
	err = m.client.Add(&memcache.Item{
		Key:   realkey,
		Value: []byte(strconv.FormatInt(delta, 10)),
	})
	if err == nil {
		return uint64(delta), nil
	} else if err != memcache.ErrNotStored {
		return 0, err
	}

	//if add returns ErrNotStored(key exists), try increment again
	return f(realkey, uint64(delta))
}

func (m *Client) TryLockOnce(key string) (bool, error) {
	err := m.client.Add(&memcache.Item{
		Key: m.KeyFunc(key),
	})

	if err == nil {
		return true, nil
	} else if err == memcache.ErrNotStored {
		return false, nil
	} else {
		return false, err
	}
}

func (m *Client) Unlock(key string) (bool, error) {
	return m.Remove(key)
}

func (m *Client) GetWithFlags(key string) (bool, []byte, uint32, error) {
	start := time.Now()
	item, err := m.client.Get(m.KeyFunc(key))

	if err == memcache.ErrCacheMiss {
		memProm.Inc("get", "miss")
		memProm.HandleTime(start, "get", "miss")
		return false, nil, 0, nil
	} else if err != nil {
		memProm.Inc("get", "failed")
		memProm.HandleTime(start, "get", "failed")
		return false, nil, 0, nil
	}
	memProm.Inc("get", "hit")
	memProm.HandleTime(start, "get", "hit")
	return true, item.Value, item.Flags, nil
}

func (m *Client) GetMultiWithFlags(keys []string) (map[string][]byte, map[string]uint32, error) {
	start := time.Now()
	var newkey string
	keysWithPrefix := make([]string, len(keys))
	keyMap := make(map[string]string, len(keys))
	for i, k := range keys {
		newkey = m.KeyFunc(k)
		keysWithPrefix[i] = newkey
		keyMap[newkey] = k
	}

	ret := make(map[string][]byte, len(keys))
	retFlags := make(map[string]uint32, len(keys))
	itemMap, err := m.client.GetMulti(keysWithPrefix)
	if err != nil {
		memProm.Inc("get_multi", "failed")
		memProm.HandleTime(start, "get_multi", "failed")
		return ret, retFlags, err
	}

	for k, v := range itemMap {
		ret[keyMap[k]] = v.Value
		retFlags[keyMap[k]] = v.Flags
	}
	memProm.Inc("get_multi", "hit")
	memProm.HandleTime(start, "get_multi", "hit")
	return ret, retFlags, nil
}

func (m *Client) SetWithFlags(key string, value []byte, ttl time.Duration, flags uint32) error {
	start := time.Now()
	if ttl > 30*24*time.Hour {
		ttl = 30 * 24 * time.Hour
	}
	item := &memcache.Item{
		Key:        m.KeyFunc(key),
		Value:      value,
		Expiration: int32(ttl.Seconds()),
		Flags:      flags,
	}
	if err := m.client.Set(item); err != nil {
		memProm.Inc("set", "failed")
		memProm.HandleTime(start, "set", "failed")
		return err
	}
	memProm.Inc("set", "hit")
	memProm.HandleTime(start, "set", "hit")
	return nil
}
