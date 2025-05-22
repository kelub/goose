package rate_limit

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/go-redis/redis_rate/v9"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"

	"github.com/kelub/goose/pkg/prom"
)

var (
	rateLimitProm = prom.NewPromVec("rate_limit").
		Counter("total_handled", "Total number of request made.",
			[]string{"service", "name", "limit"}).
		Histogram("duration_seconds", "Bucketed histogram of api response time duration",
			[]string{"service", "name", "limit"},
			prometheus.ExponentialBuckets(0.001, 2.0, 15),
		)
)

// key rate:<service_name>:<type_name>:<version>:<key>

var (
	rdb *redis.Client
)

var (
	// lock only for rateLimits
	lock       sync.RWMutex
	rateLimits = make(map[string]RateLimiter)

	defaultLimit   = redis_rate.PerSecond(1)
	defaultVersion = "v1"
)

type RateLimiter interface {
	Allow(ctx context.Context, key string) (bool, error)
	AllowN(ctx context.Context, key string, n int) (bool, error)
}

type RateLimit struct {
	ServiceName, TypeName string
	Limiter               *redis_rate.Limiter

	Opts *RateLimiterOptions
}

type RateLimiterOptions struct {
	Version string
	Limit   redis_rate.Limit

	prefix string
}

type Option func(*RateLimiterOptions)

func WithLimitOption(limit redis_rate.Limit) Option {
	return func(args *RateLimiterOptions) {
		args.Limit = limit
	}
}

func WithVersionOption(version string) Option {
	return func(args *RateLimiterOptions) {
		args.Version = version
	}
}

// GetRateLimit depend on rdb
func GetRateLimit(serviceName, typeName string) (RateLimiter, error) {
	var err error
	name := fmt.Sprintf("%s:%s", serviceName, typeName)
	lock.RLock()
	rl, ok := rateLimits[name]
	lock.RUnlock()
	if ok {
		return rl, nil
	}
	lock.Lock()
	rl, ok = rateLimits[name]
	if !ok {
		rl, err = NewRateLimit(rdb, serviceName, typeName)
		if err != nil {
			return nil, err
		}
		rateLimits[name] = rl
	}
	lock.Unlock()
	return rl, nil
}

// NewRateLimit depend on redis. serviceName and typeName should exists
// key rate:<service_name>:<type_name>:<version>:<consumer_key>
func NewRateLimit(redisCli *redis.Client, serviceName, typeName string, opts ...Option) (*RateLimit, error) {
	if redisCli == nil {
		return nil, errors.New("redisCli nil")
	}
	if len(serviceName) == 0 {
		return nil, errors.New("service name must exist")
	}
	if len(typeName) == 0 {
		typeName = "default"
	}

	args := &RateLimiterOptions{Version: defaultVersion}
	for _, opt := range opts {
		opt(args)
	}
	if args.Limit.IsZero() {
		args.Limit = defaultLimit
	}

	args.prefix = fmt.Sprintf("%s:%s:%s", serviceName, typeName, args.Version)

	return &RateLimit{
		ServiceName: serviceName,
		TypeName:    typeName,
		Limiter:     redis_rate.NewLimiter(redisCli),
		Opts:        args,
	}, nil
}

func (pl *RateLimit) Allow(ctx context.Context, key string) (bool, error) {
	return pl.AllowN(ctx, key, 1)
}

func (pl *RateLimit) AllowN(ctx context.Context, key string, n int) (bool, error) {
	if len(pl.ServiceName) == 0 {
		return false, errors.New("invalid parameter")
	}
	key = fmt.Sprintf("%s:%s", pl.Opts.prefix, key)
	now := time.Now()

	res, err := pl.Limiter.AllowN(ctx, key, pl.Opts.Limit, n)

	if err != nil {
		rateLimitProm.Inc(pl.ServiceName, pl.TypeName, "failed")
		return false, errors.New("limit failed")
	}

	logrus.Debugf("%+v", res)

	if res.Allowed == 0 {
		rateLimitProm.Inc(pl.ServiceName, pl.TypeName, "limit")
		rateLimitProm.HandleTime(now, pl.ServiceName, pl.TypeName, "limit")
	} else {
		rateLimitProm.Inc(pl.ServiceName, pl.TypeName, "ok")
		rateLimitProm.HandleTime(now, pl.ServiceName, pl.TypeName, "ok")
	}
	logrus.Debug("e:", time.Since(now).Seconds())
	return res.Allowed != 0, nil
}

func (pl *RateLimit) SetLimit(limit redis_rate.Limit) {
	if pl == nil || pl.Opts == nil {
		return
	}
	pl.Opts.Limit = limit
}
