// Package kafka provides a kafka producer
package kafka

import (
	"github.com/IBM/sarama"
	log "github.com/sirupsen/logrus"
)

// Producer kafka producer interface
type Producer interface {
	Publish(topic string, data []byte, key *string) error
	Close() error
}

// AsyncProducer kafka async producer
type AsyncProducer struct {
	producer sarama.AsyncProducer
}

// AsyncProducerOption kafka async producer option
type AsyncProducerOption struct {
	KafkaHost           []string
	KafkaConfigFunc     func(*sarama.Config)
	ErrorCallbackFunc   func(*sarama.ProducerError)
	SuccessCallbackFunc func(*sarama.ProducerMessage)
}

// NewAsyncProducer create kafka async producer
// Parameters:
// - opt: the option for the async producer.
//
// Returns:
// - *AsyncProducer: the async producer.
// - error: an error if one occurs.
func NewAsyncProducer(opt AsyncProducerOption) (*AsyncProducer, error) {
	cfg := sarama.NewConfig()
	cfg.Producer.Return.Successes = true
	cfg.Producer.Return.Errors = true

	if opt.KafkaConfigFunc != nil {
		opt.KafkaConfigFunc(cfg)
	}
	ap := &AsyncProducer{}
	producer, err := sarama.NewAsyncProducer(opt.KafkaHost, cfg)
	if err != nil {
		return ap, err
	}
	ap.producer = producer

	go func() {
		for err := range producer.Errors() {
			log.WithFields(log.Fields{
				"topic": err.Msg.Topic,
			}).WithError(err).Warn("send message failed")
			if opt.ErrorCallbackFunc != nil {
				opt.ErrorCallbackFunc(err)
			}
		}
	}()

	go func() {
		for msg := range producer.Successes() {
			log.WithFields(log.Fields{
				"topic":     msg.Topic,
				"partition": msg.Partition,
				"offset":    msg.Offset,
			}).Info("send message success")
			if opt.SuccessCallbackFunc != nil {
				opt.SuccessCallbackFunc(msg)
			}
		}
	}()

	return ap, nil
}

// Close close kafka async producer
// Returns:
// - error: an error if one occurs.
func (ap *AsyncProducer) Close() error {
	return ap.producer.Close()
}

// Publish publish message to kafka
// Parameters:
// - topic: the topic to use.
// - message: the message to use.
// - key: the key to use.
//
// Returns:
// - error: an error if one occurs.
func (ap *AsyncProducer) Publish(topic string, message []byte, key ...string) error {
	msg := &sarama.ProducerMessage{Topic: topic, Value: sarama.ByteEncoder(message)}
	if len(key) != 0 {
		msg.Key = sarama.StringEncoder(key[0])
	}
	ap.producer.Input() <- msg
	return nil
}
