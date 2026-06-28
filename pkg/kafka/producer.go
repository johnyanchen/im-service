package kafka

import (
	"encoding/json"

	"github.com/IBM/sarama"
)

const TopicMessageFanout = "message_fanout"

type FanoutEvent struct {
	MessageID        int64  `json:"message_id"`
	ConversationID   int64  `json:"conversation_id"`
	ConversationType string `json:"conversation_type"`
	ConversationName string `json:"conversation_name"`
	FromID           int64  `json:"from_id"`
	FromUsername     string `json:"from_username"`
	Content          string `json:"content"`
	CreatedAt        int64  `json:"created_at"`
}

type Producer struct {
	producer sarama.SyncProducer
}

func NewProducer(brokers []string) (*Producer, error) {
	cfg := sarama.NewConfig()
	cfg.Producer.Return.Successes = true
	p, err := sarama.NewSyncProducer(brokers, cfg)
	if err != nil {
		return nil, err
	}
	return &Producer{producer: p}, nil
}

func (p *Producer) PublishFanout(event *FanoutEvent) error {
	data, err := json.Marshal(event)
	if err != nil {
		return err
	}
	_, _, err = p.producer.SendMessage(&sarama.ProducerMessage{
		Topic: TopicMessageFanout,
		Value: sarama.ByteEncoder(data),
	})
	return err
}

func (p *Producer) Close() error {
	return p.producer.Close()
}
