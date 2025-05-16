package sqs

import (
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go/service/sqs"
	log "github.com/sirupsen/logrus"
)

type AttrsOption struct {
	key  string
	call func(m *sqs.Message, attr *Attribute)
}

type Attribute struct {
	ApproximateReceiveCount          string `json:"approximate_receive_count,omitempty"`
	ApproximateFirstReceiveTimestamp string `json:"approximate_first_receive_timestamp,omitempty"`
	MessageDeduplicationId           string `json:"message_deduplication_id,omitempty"`
	MessageGroupId                   string `json:"message_group_id,omitempty"`
	SenderId                         string `json:"sender_id,omitempty"`
	SentTimestamp                    string `json:"sent_timestamp,omitempty"`
	SequenceNumber                   string `json:"sequence_number,omitempty"`
}

func (attr *Attribute) TransDDTraceMap(m map[string]string) {
	if m == nil {
		return
	}
	// use json serialization and deserialization will cause escape,
	// when traversing the object, the object will also escape, only a few, manually
	if attr.ApproximateReceiveCount != "" {
		// datadog requires the format of the structure to be "structure name.attribute name"
		m["attribute.approximate_receive_count"] = attr.ApproximateReceiveCount
	}
	if attr.ApproximateFirstReceiveTimestamp != "" {
		m["attribute.approximate_first_receive_timestamp"] = attr.ApproximateFirstReceiveTimestamp
	}
	if attr.MessageDeduplicationId != "" {
		m["attribute.message_deduplication_id"] = attr.MessageDeduplicationId
	}
	if attr.MessageGroupId != "" {
		m["attribute.message_group_id"] = attr.MessageGroupId
	}
	if attr.SenderId != "" {
		m["attribute.sender_id"] = attr.SenderId
	}
	if attr.SentTimestamp != "" {
		m["attribute.sent_timestamp"] = attr.SentTimestamp
	}
	if attr.SequenceNumber != "" {
		m["attribute.sequence_number"] = attr.SequenceNumber
	}
}

func WithApproximateReceiveCount() *AttrsOption {
	return &AttrsOption{
		key: "ApproximateReceiveCount",
		call: func(m *sqs.Message, attr *Attribute) {
			if m.Attributes["ApproximateReceiveCount"] != nil {
				attr.ApproximateReceiveCount = *m.Attributes["ApproximateReceiveCount"]
			}
		},
	}
}

func WithApproximateFirstReceiveTimestamp() *AttrsOption {
	return &AttrsOption{
		key: "ApproximateFirstReceiveTimestamp",
		call: func(m *sqs.Message, attr *Attribute) {
			if m.Attributes["ApproximateFirstReceiveTimestamp"] != nil {
				timestampStr := *m.Attributes["ApproximateFirstReceiveTimestamp"]
				timestamp, err := strconv.ParseInt(timestampStr, 0, 64)
				if err != nil {
					log.WithError(err).Warning("with first receive timestamp")
					return
				}
				attr.ApproximateFirstReceiveTimestamp = time.Unix(0, timestamp*1e6).String()
			}
		},
	}
}

func WithMessageDeduplicationId() *AttrsOption {
	return &AttrsOption{
		key: "MessageDeduplicationId",
		call: func(m *sqs.Message, attr *Attribute) {
			if m.Attributes["MessageDeduplicationId"] != nil {
				attr.MessageDeduplicationId = *m.Attributes["MessageDeduplicationId"]
			}
		},
	}
}

func WithMessageGroupId() *AttrsOption {
	return &AttrsOption{
		key: "MessageGroupId",
		call: func(m *sqs.Message, attr *Attribute) {
			if m.Attributes["MessageGroupId"] != nil {
				attr.MessageGroupId = *m.Attributes["MessageGroupId"]
			}
		},
	}
}

func WithSenderId() *AttrsOption {
	return &AttrsOption{
		key: "SenderId",
		call: func(m *sqs.Message, attr *Attribute) {
			if m.Attributes["SenderId"] != nil {
				attr.SenderId = *m.Attributes["SenderId"]
			}
		},
	}
}

func WithSentTimestamp() *AttrsOption {
	return &AttrsOption{
		key: "SentTimestamp",
		call: func(m *sqs.Message, attr *Attribute) {
			if m.Attributes["SentTimestamp"] != nil {
				timestampStr := *m.Attributes["SentTimestamp"]
				timestamp, err := strconv.ParseInt(timestampStr, 0, 64)
				if err != nil {
					log.WithError(err).Warning("with sent timestamp")
					return
				}
				attr.SentTimestamp = time.Unix(0, timestamp*1e6).String()
			}
		},
	}
}

func WithSequenceNumber() *AttrsOption {
	return &AttrsOption{
		key: "SequenceNumber",
		call: func(m *sqs.Message, attr *Attribute) {
			if m.Attributes["SequenceNumber"] != nil {
				attr.SequenceNumber = *m.Attributes["SequenceNumber"]
			}
		},
	}
}

func defaultTrace() []*AttrsOption {
	return []*AttrsOption{
		WithApproximateReceiveCount(),
		WithApproximateFirstReceiveTimestamp(),
		WithMessageDeduplicationId(),
		WithMessageGroupId(),
		WithSenderId(),
		WithSentTimestamp(),
		WithSequenceNumber(),
	}
}
