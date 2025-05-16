// Package kafka provides a kafka consumer
package kafka

import (
	"context"
	"fmt"
	stdlog "log"
	"os"
	"strings"
	"time"

	"github.com/IBM/sarama"
	log "github.com/sirupsen/logrus"
)

// Consumer kafka consumer interface
type Consumer struct {
	kafkaHost          []string
	config             *sarama.Config
	topics             []string
	groupName          string
	messageProcessFunc func(*sarama.ConsumerMessage) error
	processErrorFunc   func(*sarama.ConsumerMessage, error)
	backOffDuration    time.Duration
}

// ConsumerOption kafka consumer option
type ConsumerOption struct {
	KafkaHost          []string
	KafkaConfigFunc    func(*sarama.Config)
	Topics             []string
	GroupName          string
	BackOffDuration    time.Duration
	MessageProcessFunc func(*sarama.ConsumerMessage) error
	ProcessErrorFunc   func(*sarama.ConsumerMessage, error)
}

// NewConsumer create kafka consumer
// Parameters:
// - opt: the option for the consumer.
//
// Returns:
// - *Consumer: the consumer.
// - error: an error if one occurs.
func NewConsumer(opt ConsumerOption) (*Consumer, error) {
	sarama.Logger = stdlog.New(os.Stderr, "", stdlog.LstdFlags)

	consumer := &Consumer{
		topics:             opt.Topics,
		groupName:          opt.GroupName,
		kafkaHost:          opt.KafkaHost,
		messageProcessFunc: opt.MessageProcessFunc,
		processErrorFunc:   opt.ProcessErrorFunc,
	}
	if len(consumer.groupName) == 0 || len(consumer.topics) < 1 || len(consumer.kafkaHost) < 1 {
		panic(fmt.Sprintf("kafka config invalid, GroupId:%s ListenTopic:%s KafkaServer:%v\n", consumer.groupName, consumer.topics, consumer.kafkaHost))
	}
	cfg := sarama.NewConfig()
	cfg.Consumer.Return.Errors = true
	cfg.Consumer.Offsets.Initial = sarama.OffsetNewest
	cfg.Version = sarama.V1_1_1_0
	cfg.ClientID = opt.GroupName
	if opt.KafkaConfigFunc != nil {
		opt.KafkaConfigFunc(cfg)
	}
	consumer.config = cfg

	return consumer, nil
}

func (Consumer) Setup(_ sarama.ConsumerGroupSession) error {
	return nil
}

func (Consumer) Cleanup(_ sarama.ConsumerGroupSession) error {
	return nil
}

func (h *Consumer) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		log.WithFields(log.Fields{
			"topic":     msg.Topic,
			"partition": msg.Partition,
			"offset":    msg.Offset,
			"time":      msg.Timestamp.Format(time.RFC3339),
		}).Info("receive message")
		log.WithFields(log.Fields{
			"value": string(msg.Value),
		}).Debug("receive message")

		err := h.messageProcessFunc(msg)
		sess.MarkMessage(msg, "")
		if err != nil {
			log.WithError(err).Error("process msg fail")
			if h.processErrorFunc != nil {
				h.processErrorFunc(msg, err)
			}
		}
	}
	return nil
}

func (h *Consumer) Run(ctx context.Context) {
	for {
		err := h.run(ctx)
		if err == context.Canceled {
			log.Info("context canceled")
			break
		}
		log.WithError(err).Warn("Listener down, restart after " + h.backOffDuration.String())
		time.Sleep(h.backOffDuration)
	}
}

func (h *Consumer) run(ctx context.Context) error {
	log.WithFields(log.Fields{
		"group":  h.groupName,
		"topics": strings.Join(h.topics, ","),
		"hosts":  strings.Join(h.kafkaHost, ","),
	}).Info("kafka config")

	client, err := sarama.NewClient(h.kafkaHost, h.config)
	if err != nil {
		return err
	}
	defer client.Close()

	group, err := sarama.NewConsumerGroupFromClient(h.groupName, client)
	if err != nil {
		return err
	}

	log.Info("begin to consume ...")
	for {
		err = group.Consume(ctx, h.topics, h)
		if err != nil {
			return err
		}
	}
}
