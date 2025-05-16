package sqs

import (
	"context"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/aws/aws-sdk-go/service/sqs/sqsiface"
	. "github.com/smartystreets/goconvey/convey"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/tracer"
)

type mockSQSClient struct {
	sqsiface.SQSAPI

	receiveMessageWithContext     func(aws.Context, *sqs.ReceiveMessageInput, ...request.Option) (*sqs.ReceiveMessageOutput, error)
	deleteMessageBatchWithContext func(aws.Context, *sqs.DeleteMessageBatchInput, ...request.Option) (*sqs.DeleteMessageBatchOutput, error)
	deleteMessageBatch            func(*sqs.DeleteMessageBatchInput) (*sqs.DeleteMessageBatchOutput, error)
}

func (m mockSQSClient) ReceiveMessageWithContext(ctx aws.Context, input *sqs.ReceiveMessageInput, opt ...request.Option) (*sqs.ReceiveMessageOutput, error) {
	return m.receiveMessageWithContext(ctx, input, opt...)
}

func (m mockSQSClient) DeleteMessageBatchWithContext(ctx aws.Context, input *sqs.DeleteMessageBatchInput, opt ...request.Option) (*sqs.DeleteMessageBatchOutput, error) {
	return m.deleteMessageBatchWithContext(ctx, input, opt...)
}

func (m mockSQSClient) DeleteMessageBatch(input *sqs.DeleteMessageBatchInput) (*sqs.DeleteMessageBatchOutput, error) {
	return m.deleteMessageBatch(input)
}

func TestConsumer_Run(t *testing.T) {
	cfg := GetDefaultConsumerConfig()
	cfg.AwsRegion = "Region"
	cfg.AwsAccessKeyId = "AwsAccessKeyId"
	cfg.AwsSecretAccessKey = "AwsSecretAccessKey"
	cfg.VisibilityTimeout = 10
	cfg.WaitTimeSeconds = 2
	cfg.MaxNumberOfMessages = 10
	cfg.SleepInterval = 1
	cfg.HeartbeatInterval = 5
	cfg.QueueUrl = "a"
	consumer, err := NewConsumer(cfg)
	if err != nil {
		panic(err)
	}
	ctx, cancel := context.WithCancel(context.Background())

	consumer.client = mockSQSClient{
		receiveMessageWithContext: func(ctx aws.Context, input *sqs.ReceiveMessageInput, opt ...request.Option) (*sqs.ReceiveMessageOutput, error) {
			Convey("x", t, func() {
				So(aws.StringValue(input.QueueUrl), ShouldEqual, cfg.QueueUrl)
			})
			time.Sleep(200 * time.Millisecond)
			return &sqs.ReceiveMessageOutput{
				Messages: []*sqs.Message{

					&sqs.Message{MessageId: aws.String(time.Now().String()),
						ReceiptHandle: aws.String("ReceiptHandle")},
				}}, nil
		},

		deleteMessageBatchWithContext: func(ctx aws.Context, input *sqs.DeleteMessageBatchInput, opt ...request.Option) (*sqs.DeleteMessageBatchOutput, error) {
			Convey("x", t, func() {
				So(aws.StringValue(input.QueueUrl), ShouldEqual, cfg.QueueUrl)
			})
			return nil, nil
		},
		deleteMessageBatch: func(input *sqs.DeleteMessageBatchInput) (*sqs.DeleteMessageBatchOutput, error) {
			Convey("x", t, func() {
				So(aws.StringValue(input.QueueUrl), ShouldEqual, cfg.QueueUrl)
			})
			return nil, nil
		},
	}
	consumerMessage := consumer.Run(ctx, cancel)

	go func() {
		select {
		case <-time.After(3 * time.Second):
			cancel()
			return
		}
	}()

	for {
		select {
		case m := <-consumerMessage:
			m.Delete()
		case <-ctx.Done():
			time.Sleep(1 * time.Second)
			return
		}
	}
}

func TestConsumerMessage_AddDDTrace(t *testing.T) {
	cfg := GetDefaultConsumerConfig()
	cfg.AwsRegion = "Region"
	cfg.AwsAccessKeyId = "AwsAccessKeyId"
	cfg.AwsSecretAccessKey = "AwsSecretAccessKey"
	cfg.VisibilityTimeout = 10
	cfg.WaitTimeSeconds = 2
	cfg.MaxNumberOfMessages = 10
	cfg.SleepInterval = 1
	cfg.HeartbeatInterval = 5
	cfg.QueueUrl = "a"

	consumer, err := NewConsumer(cfg)
	if err != nil {
		panic(err)
	}

	attrs := make(map[string]*string, 0)
	attrs["ApproximateReceiveCount"] = aws.String("ApproximateReceiveCount")
	attrs["ApproximateFirstReceiveTimestamp"] = aws.String("1636354075694")
	attrs["MessageDeduplicationId"] = aws.String("MessageDeduplicationId")
	attrs["MessageGroupId"] = aws.String("MessageGroupId")
	attrs["SenderId"] = aws.String("SenderId")
	attrs["SentTimestamp"] = aws.String("SentTimestamp")
	attrs["SequenceNumber"] = aws.String("SequenceNumber")

	msg := ConsumerMessage{consumer: consumer,
		message: &sqs.Message{Attributes: attrs, MessageId: aws.String("123")}}

	consumer.TraceInterceptor(
		WithMessageGroupId(),
		WithSequenceNumber(),
		WithApproximateReceiveCount(),
	)

	Convey("test add ddtrace", t, func() {
		So(len(consumer.attrOpts), ShouldEqual, 4)
		_ = msg.AddDDTrace(context.Background(), func(span tracer.Span) {})
		attr := msg.GetTraceAttrs()
		So(attr.ApproximateReceiveCount, ShouldBeBlank)
		So(attr.MessageGroupId, ShouldBeBlank)
		So(attr.SentTimestamp, ShouldBeBlank)
		So(attr.SequenceNumber, ShouldBeBlank)
		So(attr.ApproximateFirstReceiveTimestamp, ShouldNotBeBlank)
		So(attr.MessageDeduplicationId, ShouldNotBeBlank)
		So(attr.MessageGroupId, ShouldBeBlank)
		So(attr.SenderId, ShouldNotBeBlank)
		So(attr.SentTimestamp, ShouldBeBlank)
		So(attr.SequenceNumber, ShouldBeBlank)
	})
}
