package pubsub

import (
	"context"
)

type Publisher interface {
	Publish(ctx context.Context, topic string, data []byte) error
	Close() error
}
