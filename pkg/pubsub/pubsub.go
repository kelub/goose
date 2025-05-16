package pubsub

type Publisher interface {
	Publish(topic string, data []byte, opts ...PublishOption) error
}

type PublishOptions struct {
	DelaySeconds int64
}

type PublishOption func(*PublishOptions)

// PQWithDelay only for sqs
// seconds range is 0-900
func PQWithDelay(seconds int64) PublishOption {
	return func(args *PublishOptions) {
		if seconds < 0 {
			seconds = 0
		}
		if seconds > 900 {
			seconds = 900
		}
		args.DelaySeconds = seconds
	}
}
