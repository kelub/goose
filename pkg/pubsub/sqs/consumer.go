package sqs

import (
	"context"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/aws/aws-sdk-go/service/sqs/sqsiface"
	"github.com/sirupsen/logrus"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/tracer"

	"github.com/kelub/goose/pkg/prom"
)

var (
	HalfHour int64 = 30 * 60 * 60
)

var (
	awsInFlightProm  = prom.NewPromVec("goose").Gauge("in_flight", "in flight", []string{"name"})
	awsHeartbeatProm = prom.NewPromVec("goose").Gauge("heard_beat", "heard_beat", []string{"name"})
)

// Consumer is a consumer for SQS
type Consumer struct {
	// client sqs client
	client sqsiface.SQSAPI
	// cfg consumer config
	cfg Config
	// queueUrl sqs queue url
	queueUrl string
	// deleteMsgEntryCh delete message entry channel
	deleteMsgEntryCh chan *sqs.DeleteMessageBatchRequestEntry
	// attrOpts attribute options
	attrOpts []*AttrsOption
	// cancel context cancel
	cancel context.CancelFunc
}

// ConsumerMessage is a consumer message for SQS
type ConsumerMessage struct {
	// consumer consumer
	consumer *Consumer
	// message message
	message *sqs.Message
	// span span
	span *tracer.Span
}

// NewConsumer creates a new consumer for SQS
func NewConsumer(cfg Config) (*Consumer, error) {
	var err error
	if cfg.AwsRegion == "" || cfg.AwsAccessKeyId == "" || cfg.AwsSecretAccessKey == "" {
		panic("sqs config invalid")
	}
	if cfg.DeleteMessageBatchSize <= 0 || cfg.DeleteMessageBatchSize > 10 {
		cfg.DeleteMessageBatchSize = 10
	}

	c := &Consumer{
		cfg:              cfg,
		queueUrl:         cfg.QueueUrl,
		deleteMsgEntryCh: make(chan *sqs.DeleteMessageBatchRequestEntry),
	}
	sess, err := session.NewSession()
	if err != nil {
		return nil, err
	}
	c.client = sqs.New(sess, &aws.Config{
		Region:      aws.String(cfg.AwsRegion),
		Credentials: credentials.NewStaticCredentials(cfg.AwsAccessKeyId, cfg.AwsSecretAccessKey, ""),
		MaxRetries:  aws.Int(3),
	})
	if c.client == nil {
		panic("init sqs client error")
	}
	return c, nil
}

// Run run sqs receive message
func (c *Consumer) Run(ctx context.Context, cancel context.CancelFunc) <-chan *ConsumerMessage {
	output := make(chan *ConsumerMessage)
	go c.deleteMessageLoop(ctx)
	go func(consumer *Consumer, output chan *ConsumerMessage) {
		defer close(output)
		for {
			select {
			case <-ctx.Done():
				return
			default:
				msg, err := c.client.ReceiveMessageWithContext(ctx, &sqs.ReceiveMessageInput{
					WaitTimeSeconds:       aws.Int64(c.cfg.WaitTimeSeconds),
					QueueUrl:              aws.String(c.queueUrl),
					MaxNumberOfMessages:   aws.Int64(c.cfg.MaxNumberOfMessages),
					VisibilityTimeout:     aws.Int64(c.cfg.VisibilityTimeout),
					AttributeNames:        aws.StringSlice([]string{"All"}),
					MessageAttributeNames: aws.StringSlice([]string{"All"}),
				})
				if err != nil {
					logrus.WithError(err).Warn("sqs receive message failed, cancel ctx")
					cancel()
					continue
				}
				if len(msg.Messages) == 0 {
					logrus.Debugf("no messages found. sleeping for %s", c.cfg.SleepInterval)
					time.Sleep(c.cfg.SleepInterval)
					continue
				}
				logrus.Debugf("found %d messages", len(msg.Messages))
				for _, m := range msg.Messages {
					output <- &ConsumerMessage{
						consumer: consumer,
						message:  m,
						span:     nil,
					}
					awsInFlightProm.Inc("name")
				}
			}
		}
	}(c, output)
	return output
}

// deleteMessage delete message
func (c *Consumer) deleteMessage(batchEntries []*sqs.DeleteMessageBatchRequestEntry) {
	batchInput := &sqs.DeleteMessageBatchInput{
		QueueUrl: aws.String(c.queueUrl),
	}
	batchInput.Entries = batchEntries
	_, err := c.client.DeleteMessageBatch(batchInput)
	if err != nil {
		logrus.WithError(err).Warn("batch delete message failed")
	}
}

// deleteMessageLoop is a loop that deletes messages from the queue in batches.
// It uses a ticker to check the queue for messages and delete them in batches.
// It also uses a channel to send the messages to be deleted.
func (c *Consumer) deleteMessageLoop(ctx context.Context) {
	var checkTicker = time.NewTicker(c.cfg.DeleteMessageBatchInterval)
	defer checkTicker.Stop()
	var batchEntries = make([]*sqs.DeleteMessageBatchRequestEntry, 0, c.cfg.DeleteMessageBatchSize)
	for {
		select {
		case <-checkTicker.C:
			if len(batchEntries) > 0 {
				batchInput := &sqs.DeleteMessageBatchInput{
					QueueUrl: aws.String(c.queueUrl),
				}
				logrus.WithFields(logrus.Fields{
					"type": "checkTicker",
				}).Debugf("DeleteMessageBatch Entries len[%d]", len(batchInput.Entries))
				go c.deleteMessage(batchEntries)

				batchEntries = make([]*sqs.DeleteMessageBatchRequestEntry, 0, c.cfg.DeleteMessageBatchSize)
			}
		case batchMsg, ok := <-c.deleteMsgEntryCh:
			if !ok {
				logrus.Info("deleteMsgEntryCh closed")
				continue
			}
			batchEntries = append(batchEntries, batchMsg)
			if len(batchEntries) >= c.cfg.DeleteMessageBatchSize {
				batchInput := &sqs.DeleteMessageBatchInput{
					QueueUrl: aws.String(c.queueUrl),
				}
				logrus.WithFields(logrus.Fields{
					"type": "batchSize",
				}).Debugf("DeleteMessageBatch Entries len[%d]", len(batchInput.Entries))
				go c.deleteMessage(batchEntries)

				batchEntries = make([]*sqs.DeleteMessageBatchRequestEntry, 0, c.cfg.DeleteMessageBatchSize)
			}

		// clear before close
		case <-ctx.Done():
			if len(batchEntries) > 0 {
				batchInput := &sqs.DeleteMessageBatchInput{
					QueueUrl: aws.String(c.queueUrl),
				}
				logrus.WithFields(logrus.Fields{
					"type": "ctx cancel",
				}).Debugf("DeleteMessageBatch Entries len[%d]", len(batchInput.Entries))
				c.deleteMessage(batchEntries)

				batchEntries = make([]*sqs.DeleteMessageBatchRequestEntry, 0, c.cfg.DeleteMessageBatchSize)
			}
			return
		}
	}
}

// TraceInterceptor add monitoring attributes, parameters are attributes that do not need to be monitored
func (c *Consumer) TraceInterceptor(disableOpts ...*AttrsOption) {
	if len(c.attrOpts) == 0 {
		c.attrOpts = defaultTrace()
	}
	merge := make([]*AttrsOption, 0)
	// merge, remove existing functions
	for _, old := range c.attrOpts {
		var exist bool
		for _, opt := range disableOpts {
			if old.key == opt.key {
				exist = true
				break
			}
		}
		if !exist {
			merge = append(merge, old)
		}
	}
	c.attrOpts = merge
}

// Message get message
func (m *ConsumerMessage) Message() *sqs.Message {
	return m.message
}

// ApproximateReceiveCount get approximate receive count
func (m *ConsumerMessage) ApproximateReceiveCount() *string {
	return m.message.Attributes["ApproximateReceiveCount"]
}

// HeartbeatProcessFunc heartbeat process function
func (m *ConsumerMessage) HeartbeatProcessFunc(ctx context.Context, done chan struct{}) {
	var secondTotal int64
	heartbeatTicker := time.NewTicker(m.consumer.cfg.HeartbeatInterval)
	defer heartbeatTicker.Stop()
	for {
		select {
		case <-heartbeatTicker.C:
			select {
			case <-done:
				return
			case <-ctx.Done():
				return
			default:
			}
			if secondTotal >= HalfHour {
				logrus.Warnf("sqs message execution time is more than %d s", HalfHour)
			}
			secondTotal = int64(m.consumer.cfg.HeartbeatInterval.Seconds()) + secondTotal
			if err := m.ChangeMessageVisibility(ctx, m.consumer.cfg.VisibilityTimeout); err != nil {
				logrus.WithError(err).Warn("change message visibility failed")
				return
			}
		case <-done:
			return
		case <-ctx.Done():
			return
		}
	}
}

// ChangeMessageVisibility changes the visibility timeout of a message
// from the specified queue.
// The message visibility timeout is the length of time during which a message
// is invisible to other consumers after it is sent to the queue.
func (m *ConsumerMessage) ChangeMessageVisibility(ctx context.Context, seconds int64) error {
	_, err := m.consumer.client.ChangeMessageVisibilityWithContext(ctx, &sqs.ChangeMessageVisibilityInput{
		QueueUrl:          aws.String(m.consumer.queueUrl),
		ReceiptHandle:     m.message.ReceiptHandle,
		VisibilityTimeout: aws.Int64(seconds),
	})
	if m.span != nil {
		(*m.span).Finish()
	}
	return err
}

// Delete send delete channel
func (m *ConsumerMessage) Delete() {
	defer awsInFlightProm.Dec("name")
	m.consumer.deleteMsgEntryCh <- &sqs.DeleteMessageBatchRequestEntry{
		Id:            m.message.MessageId,
		ReceiptHandle: m.message.ReceiptHandle,
	}
	if m.span != nil {
		(*m.span).Finish()
	}
}

// GetTraceAttrs get attributes, default returns an empty object,
// you need to call TraceInterceptor first to get the data
func (m *ConsumerMessage) GetTraceAttrs() *Attribute {
	attr := &Attribute{}
	if m.Message() == nil {
		return attr
	}

	for _, opt := range m.consumer.attrOpts {
		opt.call(m.Message(), attr)
	}
	return attr
}

// AddDDTrace add sqs monitoring
func (m *ConsumerMessage) AddDDTrace(ctx context.Context, callback func(span tracer.Span)) context.Context {
	var sqsSpan tracer.Span
	sqsSpan, ctx = tracer.StartSpanFromContext(ctx, "SQS.Consumer")
	sqsSpan.SetTag("message_id", *m.Message().MessageId)

	attrs := make(map[string]string)
	m.GetTraceAttrs().TransDDTraceMap(attrs)
	for k, v := range attrs {
		sqsSpan.SetTag(k, v)
	}
	m.span = &sqsSpan
	callback(*m.span)
	return ctx
}

// SQSMessage sqs message
type SQSMessage struct {
	Body          string `json:"body"`
	MD5OfBody     string `json:"MD5_of_body"`
	MessageId     string `json:"message_id"`
	ReceiptHandle string `json:"receipt_handle"`
}

// Class get class
func (*SQSMessage) Class() string {
	return "sqs"
}

// NewSQSMessage new sqs message
func NewSQSMessage(m *ConsumerMessage) *SQSMessage {
	msg := m.Message()
	if msg == nil {
		return nil
	}
	return &SQSMessage{
		Body:          aws.StringValue(msg.Body),
		MD5OfBody:     aws.StringValue(msg.MD5OfBody),
		MessageId:     aws.StringValue(msg.MessageId),
		ReceiptHandle: aws.StringValue(msg.ReceiptHandle),
	}
}
