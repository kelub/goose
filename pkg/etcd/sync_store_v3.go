// Package etcd provides a client for interacting with an etcd cluster.
// It provides methods to access raw client operations and simplifies configuration.
package etcd

import (
	"context"
	"path"
	"strings"
	"sync"
	"time"

	"go.etcd.io/etcd/client/v3"
)

// ETCDSyncStore is a sync store that uses etcd as the backend.
type ETCDSyncStore struct {
	data      map[string]string
	mutex     *sync.Mutex
	callbacks []func(Response)
}

// ConnectionOption is the option for the connection to the etcd cluster.
type ConnectionOption struct {
	Hosts   []string
	Timeout time.Duration
}

// NewETCDSyncStoreFromClient creates a new ETCDSyncStore from a client.
//
// Parameters:
// - ctx: the context for the operation.
// - dir: the directory to watch.
// - cli: the client to use.
// - callbacks: the callbacks to use.
//
// Returns:
// - *ETCDSyncStore: the sync store.
// - error: an error if one occurs.
func NewETCDSyncStoreFromClient(ctx context.Context, dir string, cli *ETCDV3Client, callbacks ...func(Response)) (*ETCDSyncStore, error) {
	st := &ETCDSyncStore{
		mutex:     &sync.Mutex{},
		callbacks: callbacks,
		data:      map[string]string{},
	}

	kvc := clientv3.NewKV(cli.RawClient())
	prefix := path.Join(cli.Prefix, dir) + "/"

	resp, err := kvc.Get(ctx, prefix, clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}

	st.mutex.Lock()
	// get the value from the etcd
	for _, kv := range resp.Kvs {
		k := strings.TrimPrefix(string(kv.Key), prefix)
		v := string(kv.Value)
		st.data[k] = v
		for _, cb := range st.callbacks {
			cb(Response{
				Action: "PUT",
				Key:    k,
				Value:  v,
			})
		}
	}
	st.mutex.Unlock()

	// watch from next revision
	curRevision := resp.Header.Revision + 1
	go func() {
		for wresp := range cli.WatchDir(ctx, dir, clientv3.WithRev(curRevision)) {
			st.mutex.Lock()
			if wresp.Action == "PUT" {
				st.data[wresp.Key] = wresp.Value
			} else if wresp.Action == "DELETE" {
				delete(st.data, wresp.Key)
			}
			for _, cb := range st.callbacks {
				cb(wresp)
			}
			st.mutex.Unlock()
		}
	}()

	return st, nil
}

// NewETCDSyncStore creates a new ETCDSyncStore from a client.
// This function returns a boolean indicating whether the key exists,
//
// Parameters:
// - ctx: the context for the operation.
// - dir: the directory to watch.
// - opt: the option for the connection to the etcd cluster.
//
// Returns:
// - *ETCDSyncStore: the sync store.
// - error: an error if one occurs.
func NewETCDSyncStore(ctx context.Context, dir string, opt ConnectionOption) (*ETCDSyncStore, error) {
	// create a new client
	cli, err := NewETCDV3Client(opt.Hosts, opt.Timeout)
	if err != nil {
		return nil, err
	}

	return NewETCDSyncStoreFromClient(ctx, dir, cli)

}

// Get retrieves the value for a given key from the sync store.
//
// Parameters:
// - key: the key to retrieve.
//
// Returns:
// - bool: true if the key exists, false otherwise.
// - string: the value of the key.
// - error: an error if one occurs.
func (store *ETCDSyncStore) Get(key string) (bool, string, error) {
	store.mutex.Lock()
	v, ok := store.data[key]
	store.mutex.Unlock()
	return ok, v, nil
}

// Len returns the number of keys in the sync store.
//
// Returns:
// - int: the number of keys in the sync store.
func (store *ETCDSyncStore) Len() int {
	store.mutex.Lock()
	defer store.mutex.Unlock()
	return len(store.data)
}

// Map returns a map of the keys in the sync store.
//
// Returns:
// - map[string]string: the map of the keys in the sync store.
func (store *ETCDSyncStore) Map() map[string]string {
	store.mutex.Lock()
	defer store.mutex.Unlock()
	targetMap := make(map[string]string, len(store.data))
	for k, v := range store.data {
		targetMap[k] = v
	}
	return targetMap
}
