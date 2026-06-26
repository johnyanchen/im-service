package kafka

import (
	"context"
	"encoding/json"
	"log"

	"github.com/IBM/sarama"
)

type Handler func(ctx context.Context, event *FanoutEvent) error

type Consumer struct {
	group   sarama.ConsumerGroup
	handler Handler
	topic   string
}

func NewConsumer(brokers []string, groupID string, handler Handler) (*Consumer, error) {
	cfg := sarama.NewConfig()
	cfg.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{sarama.NewBalanceStrategyRoundRobin()}
	cfg.Consumer.Offsets.Initial = sarama.OffsetNewest
	g, err := sarama.NewConsumerGroup(brokers, groupID, cfg)
	if err != nil {
		return nil, err
	}
	return &Consumer{group: g, handler: handler, topic: TopicMessageFanout}, nil
}

func (c *Consumer) Run(ctx context.Context) error {
	for {
		if err := c.group.Consume(ctx, []string{c.topic}, c); err != nil {
			return err
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
	}
}

func (c *Consumer) Setup(_ sarama.ConsumerGroupSession) error   { return nil }
func (c *Consumer) Cleanup(_ sarama.ConsumerGroupSession) error { return nil }

func (c *Consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		var event FanoutEvent
		if err := json.Unmarshal(msg.Value, &event); err != nil {
			log.Printf("kafka: unmarshal error: %v", err)
			session.MarkMessage(msg, "")
			continue
		}
		if err := c.handler(session.Context(), &event); err != nil {
			log.Printf("kafka: handler error: %v", err)
		}
		session.MarkMessage(msg, "")
	}
	return nil
}

func (c *Consumer) Close() error {
	return c.group.Close()
}
