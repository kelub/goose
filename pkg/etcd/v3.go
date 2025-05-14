// Package etcd provides a client for interacting with an etcd cluster.
// It provides methods to access raw client operations and simplifies configuration.
package etcd

import (
	"context"
	"encoding/json"
	"path"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/mvcc"
)

// DefaultTraverseOption is the default traverse option.
var DefaultTraverseOption TraverseOption

func init() {
	DefaultTraverseOption = TraverseOption{
		PageSize: 20,
		Interval: time.Second,
	}
}

// ETCDV3ClientInterface defines the interface for etcd v3 client operations
type ETCDV3ClientInterface interface {
	// RawClient returns the underlying etcd v3 client instance
	RawClient() *clientv3.Client

	// SetPrefix sets the prefix for all keys
	SetPrefix(prefix string)

	// Get retrieves the value for a given key
	Get(ctx context.Context, key string) (bool, string, error)

	// GetObject retrieves and unmarshals the value for a given key into an object
	GetObject(ctx context.Context, key string, obj interface{}) (bool, error)

	// Set sets the value for a given key
	Set(ctx context.Context, key, value string, opt *SetOption) error

	// SetObject marshals an object and sets it for a given key
	SetObject(ctx context.Context, key string, obj interface{}, opt *SetOption) error

	// Delete deletes a given key
	Delete(ctx context.Context, key string) error

	// DeleteDir deletes a directory and all its contents
	DeleteDir(ctx context.Context, dir string) (int64, error)

	// WatchDir watches a directory for changes
	WatchDir(ctx context.Context, dir string, opts ...clientv3.OpOption) <-chan Response

	// TraverseDir traverses a directory and returns its contents
	TraverseDir(ctx context.Context, dir string, opt TraverseOption) (<-chan Response, <-chan error)
}

// ETCDV3Client is a client wrapper for interacting with an etcd cluster.
// It provides methods to access raw client operations and simplifies configuration.
type ETCDV3Client struct {
	// Hosts represents the list of etcd cluster endpoints.
	Hosts []string
	// Timeout specifies the connection timeout duration.
	Timeout time.Duration
	// Prefix is an optional string prepended to all etcd keys.
	Prefix string
	// client is the underlying etcd v3 client.
	client *clientv3.Client
	// kv is the cached KV client instance.
	kv clientv3.KV
}

// Response represents a response from an etcd operation.
//
// It contains the action performed, the key, value, directory status, and an error if one occurs.
type Response struct {
	Action string
	Key    string
	Value  string
	Dir    bool
	Err    error
}

// SetOption defines options for setting values in etcd
type SetOption struct {
	TTL time.Duration
}

// TraverseOption defines the options for traversing a directory in etcd.
type TraverseOption struct {
	PageSize int64
	Interval time.Duration
}

// NewETCDV3Client creates and initializes a new ETCDV3Client.
//
// Parameters:
// - hosts: a list of etcd cluster endpoints.
// - timeout: the maximum time to wait for a connection to be established.
//
// Returns:
// - *ETCDV3Client: the initialized ETCDV3Client instance.
// - error: an error if the connection fails.
func NewETCDV3Client(hosts []string, timeout time.Duration) (*ETCDV3Client, error) {
	cfg := clientv3.Config{
		Endpoints:   hosts,
		DialTimeout: timeout,
	}

	c, err := clientv3.New(cfg)
	if err != nil {
		return nil, err
	}

	return &ETCDV3Client{
		Hosts:   hosts,
		Timeout: timeout,
		client:  c,
		kv:      clientv3.NewKV(c),
	}, nil
}

// RawClient returns the underlying etcd v3 client instance.
//
// This can be used for direct access to the etcd client for custom operations.
func (etcd *ETCDV3Client) RawClient() *clientv3.Client {
	return etcd.client
}

// SetPrefix set Prefix for client
func (etcd *ETCDV3Client) SetPrefix(prefix string) {
	if !strings.HasSuffix(prefix, "/") {
		prefix = prefix + "/"
	}
	etcd.Prefix = prefix
}

// Get retrieves the value for a given key from etcd.
// This function returns a boolean indicating whether the key exists,
//
// Parameters:
// - key: a list of etcd cluster endpoints.
// - timeout: the maximum time to wait for a connection to be established.
//
// Returns:
// - bool: true if the key exists, false otherwise.
// - string: the value of the key.
// - error: an error if one occurs.
func (etcd *ETCDV3Client) Get(ctx context.Context, key string) (bool, string, error) {
	if key == "" {
		return false, "", nil
	}

	ctx, cancel := context.WithTimeout(ctx, etcd.Timeout)
	defer cancel()

	resp, err := etcd.kv.Get(ctx, path.Join(etcd.Prefix, key))

	if err != nil {
		return false, "", err
	}

	if resp.Count == 0 {
		return false, "", nil
	}

	for _, ev := range resp.Kvs {
		return true, string(ev.Value), nil
	}

	return false, "", nil
}

// GetObject retrieves the value for a given key from etcd and unmarshals it into an object.
//
// Parameters:
// - key: the key to retrieve the value from.
// - obj: the object to unmarshal the value into.
//
// Returns:
// - bool: true if the key exists, false otherwise.
// - error: an error if one occurs.
func (etcd *ETCDV3Client) GetObject(ctx context.Context, key string, obj interface{}) (bool, error) {
	exist, value, err := etcd.Get(ctx, key)
	if err != nil || !exist {
		return exist, err
	}

	err = json.Unmarshal([]byte(value), obj)
	return exist, err
}

// Set sets the value for a given key in etcd.
//
// Parameters:
// - key: the key to set the value for.
// - value: the value to set.
// - opt: the options for the set operation.
//
// Returns:
// - error: an error if one occurs.
func (etcd *ETCDV3Client) Set(ctx context.Context, key, value string, opt *SetOption) error {
	ctx, cancel := context.WithTimeout(ctx, etcd.Timeout)
	defer cancel()

	options := []clientv3.OpOption{}
	if opt != nil && int64(opt.TTL) > 0 {
		lease := clientv3.NewLease(etcd.client)
		grantResp, err := lease.Grant(ctx, int64(opt.TTL)/1000000000)
		if err != nil {
			return err
		}
		options = append(options, clientv3.WithLease(grantResp.ID))
	}

	_, err := etcd.kv.Put(ctx, path.Join(etcd.Prefix, key), value, options...)
	return err
}

// SetObject sets the value for a given key in etcd and marshals an object into a string.
//
// Parameters:
// - ctx: the context for the set operation.
// - key: the key to set the value for.
// - obj: the object to marshal and set.
// - opt: the options for the set operation.
func (etcd *ETCDV3Client) SetObject(ctx context.Context, key string, obj interface{}, opt *SetOption) error {
	js, err := json.Marshal(obj)
	if err != nil {
		return err
	}

	return etcd.Set(ctx, key, string(js), opt)
}

// Delete deletes a given key from etcd.
//
// Parameters:
// - ctx: the context for the delete operation.
// - key: the key to delete.
func (etcd *ETCDV3Client) Delete(ctx context.Context, key string) error {
	kvc := clientv3.NewKV(etcd.client)

	ctx, cancel := context.WithTimeout(ctx, etcd.Timeout)
	defer cancel()

	_, err := kvc.Delete(ctx, path.Join(etcd.Prefix, key))

	return err
}

// DeleteDir deletes a given directory from etcd.
//
// Parameters:
// - dir: the directory to delete.
//
// Returns:
// - int64: the number of keys deleted.
// - error: an error if one occurs.
func (etcd *ETCDV3Client) DeleteDir(ctx context.Context, dir string) (int64, error) {
	kvc := clientv3.NewKV(etcd.client)
	ctx, cancel := context.WithTimeout(ctx, etcd.Timeout)
	defer cancel()
	path := path.Join(etcd.Prefix, dir)
	if !strings.HasSuffix(path, "/") {
		path = path + "/"
	}

	resp, err := kvc.Delete(ctx, path, clientv3.WithPrefix())

	return resp.Deleted, err
}

// WatchDir watches a directory in etcd and returns a channel of responses.
//
// Parameters:
// - ctx: the context for the watch operation.
// - dir: the directory to watch.
// - opts: the options for the watch operation.
//
// Returns:
// - <-chan Response: a channel of responses.
func (etcd *ETCDV3Client) WatchDir(ctx context.Context, dir string, opts ...clientv3.OpOption) <-chan Response {
	ch := make(chan Response, 10)
	//copy opts
	watchOpts := []clientv3.OpOption{clientv3.WithPrefix()}
	watchOpts = append(watchOpts, opts...)
	path := path.Join(etcd.Prefix, dir)
	if !strings.HasSuffix(path, "/") {
		path = path + "/"
	}
	// watch dir indefinitely
	go func() {
		defer close(ch)
		var (
			wresp    clientv3.WatchResponse
			revision int64
		)
		for {
			select {
			case <-ctx.Done():
				log.Info("context is canceled, quit etcd watching")
				return
			default:
			}
			if revision > 0 {
				watchOpts = append(watchOpts, clientv3.WithRev(revision))
			}
			for wresp = range etcd.client.Watch(clientv3.WithRequireLeader(ctx), path, watchOpts...) {
				if wresp.Header.Revision > 0 {
					revision = wresp.Header.Revision + 1
				}
				for _, ev := range wresp.Events {
					ch <- Response{
						Action: ev.Type.String(),
						Key:    strings.TrimPrefix(string(ev.Kv.Key), path),
						Value:  string(ev.Kv.Value),
						Dir:    false,
					}
				}
			}

			// If the context "ctx" is canceled or timed out, returned "WatchChan" is closed,
			// and "WatchResponse" from this closed channel has zero events and nil "Err()".
			if wresp.Err() == nil {
				log.Info("context is canceled, quit etcd watching")
				return
			} else if wresp.Err() != nil {
				log.WithError(wresp.Err()).Warn("etcd watch failed, retrying")
				if wresp.Err() == mvcc.ErrCompacted {
					revision = wresp.CompactRevision
				} else {
					// TODO: handle this error
					log.WithError(wresp.Err()).Error("etcd watch failed, quitting")
					return
				}
			}
			// aviod busy-waiting
			time.Sleep(100 * time.Millisecond)
		}
	}()

	return ch
}

// TraverseDir traverses a directory in etcd and returns a channel of responses and an error channel.
//
// Parameters:
// - dir: the directory to traverse.
// - opt: the options for the traverse operation.
//
// Returns:
// - <-chan Response: a channel of responses.
// - <-chan error: an error channel.
func (etcd *ETCDV3Client) TraverseDir(ctx context.Context, dir string, opt TraverseOption) (<-chan Response, <-chan error) {
	ch := make(chan Response)
	errch := make(chan error)

	kvc := clientv3.NewKV(etcd.client)
	path := path.Join(etcd.Prefix, dir)
	if !strings.HasSuffix(path, "/") {
		path = path + "/"
	}

	go func() {
		defer close(errch)
		defer close(ch)

		key := path
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}
			ctx, cancel := context.WithTimeout(context.Background(), etcd.Timeout)
			resp, err := kvc.Get(
				ctx,
				key,
				clientv3.WithRange(getPrefixLastKey(path)),
				clientv3.WithLimit(opt.PageSize),
				clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend),
			)
			cancel()
			if err != nil {
				errch <- err
				break
			}
			if resp.Count == 0 {
				break
			}
			for _, ev := range resp.Kvs {
				ch <- Response{
					Key:   strings.TrimPrefix(string(ev.Key), path),
					Value: string(ev.Value),
				}
				key = string(ev.Key) + "\x00"
			}
			if !resp.More {
				break
			}

			if uint64(opt.Interval) > 0 {
				time.Sleep(opt.Interval)
			}
		}
	}()

	return ch, errch
}

// Response helper methods
func (resp Response) IsPut() bool {
	return resp.Action == "PUT"
}

func (resp Response) IsDelete() bool {
	return resp.Action == "DELETE"
}

// getPrefixLastKey returns the last key in the prefix.
func getPrefixLastKey(strkey string) string {
	key := []byte(strkey)

	end := make([]byte, len(key))
	copy(end, key)
	for i := len(end) - 1; i >= 0; i-- {
		if end[i] < 0xff {
			end[i] = end[i] + 1
			end = end[:i+1]
			return string(end)
		}
	}
	return "\x00"
}
