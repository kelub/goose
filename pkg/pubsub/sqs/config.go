package sqs

import (
	"time"

	"github.com/aws/aws-sdk-go/aws"
)

const (
	DefaultVisibilityTimeout          = 4 * 60
	DefaultWaitTimeSeconds            = 2
	DefaultMaxNumberOfMessages        = 10
	DefaultSleepInterval              = 1
	DefaultHeartbeatInterval          = 3 * 60 * time.Second
	DefaultTerminateVisibilityTimeout = true
	DefaultDeleteMessageBatchSize     = 10
	DefaultDeleteMessageBatchInterval = 2 * time.Second
)

type Config struct {
	aws.Config

	AwsRegion          string
	AwsAccessKeyId     string
	AwsSecretAccessKey string

	QueueUrl            string
	VisibilityTimeout   int64
	WaitTimeSeconds     int64
	MaxNumberOfMessages int64
	SleepInterval       time.Duration
	// HeartbeatInterval  The value should be less than VisibilityTimeout.
	// The interval between requests to extend the message visibility timeout.
	// On each heartbeat, add VisibilityTimeout to extend visibility.
	HeartbeatInterval time.Duration
	// TerminateVisibilityTimeout
	// If true, sets the message visibility timeout to 0 after a ProcessFunc error (defaults to false).
	TerminateVisibilityTimeout bool
	// DeleteMessageBatchSize
	// The maximum length of the batch delete buffer
	// if it exceeds this length or reaches the DeleteMessageBatchInterval interval, a batch delete message operation will be performed.
	// default 10 Max 10
	DeleteMessageBatchSize int
	// DeleteMessageBatchInterval
	// The interval between batch delete message operations
	// default 2s
	DeleteMessageBatchInterval time.Duration
}

type ProducerConfig struct{}

// GetDefaultConsumerConfig get default consumer config
func GetDefaultConsumerConfig() Config {
	return Config{
		VisibilityTimeout:          DefaultVisibilityTimeout,
		WaitTimeSeconds:            DefaultWaitTimeSeconds,
		MaxNumberOfMessages:        DefaultMaxNumberOfMessages,
		SleepInterval:              DefaultSleepInterval,
		HeartbeatInterval:          DefaultHeartbeatInterval,
		TerminateVisibilityTimeout: DefaultTerminateVisibilityTimeout,
		DeleteMessageBatchSize:     DefaultDeleteMessageBatchSize,
		DeleteMessageBatchInterval: DefaultDeleteMessageBatchInterval,
	}
}
