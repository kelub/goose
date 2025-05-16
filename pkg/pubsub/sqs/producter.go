package sqs

import (
	"context"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/aws/aws-sdk-go/service/sqs/sqsiface"
	"github.com/kelub/goose/pkg/pubsub"
	"github.com/sirupsen/logrus"
)

// Publisher is a publisher for SQS
type Publisher struct {
	client sqsiface.SQSAPI
}

// NewPublisher creates a new publisher for SQS
func NewPublisher(ctx context.Context, cfg Config) (pubsub.Publisher, error) {
	p := new(Publisher)
	sess, err := session.NewSession()
	if err != nil {
		return nil, err
	}
	p.client = sqs.New(sess, &aws.Config{
		Region:      aws.String(cfg.AwsRegion),
		Credentials: credentials.NewStaticCredentials(cfg.AwsAccessKeyId, cfg.AwsSecretAccessKey, ""),
	})
	if p.client == nil {
		panic("init sqs client error")
	}
	return p, nil
}

func (p *Publisher) Publish(queueUrl string, message []byte, setters ...pubsub.PublishOption) error {
	opts := &pubsub.PublishOptions{}
	for _, setter := range setters {
		setter(opts)
	}

	input := &sqs.SendMessageInput{
		MessageBody: aws.String(string(message)),
		QueueUrl:    aws.String(queueUrl),
	}

	if opts.DelaySeconds > 0 {
		input.DelaySeconds = aws.Int64(opts.DelaySeconds)
	}

	logFields := logrus.WithFields(logrus.Fields{"queue_url": queueUrl, "message": string(message)})
	output, err := p.client.SendMessage(input)
	if err != nil {
		logFields.WithError(err).Warn("sqs send message failed")
		return err
	}
	logFields.WithFields(logrus.Fields{"message_id": aws.StringValue(output.MessageId)}).Println("sqs send message successful")
	return nil
}
