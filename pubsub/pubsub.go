package pubsub

import (
	"context"
	"errors"
	"fmt"
	"runtime/debug"

	"github.com/getsentry/raven-go"
)

type Publisher interface {
	Publish(ctx context.Context, topic string, data []byte) error
	Close() error
}
