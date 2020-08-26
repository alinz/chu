package chu

import (
	"context"
	"crypto/tls"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/stan.go"
)

type NatsConsumer struct {
	messages chan *Message
	sub      stan.Subscription
}

var _ Consumer = (*NatsConsumer)(nil)
var _ Closer = (*NatsConsumer)(nil)

func (nc *NatsConsumer) Consume(ctx context.Context) (*Message, error) {
	select {
	case <-ctx.Done():
		return nil, context.Canceled
	case message := <-nc.messages:
		return message, nil
	}
}

func (nc *NatsConsumer) Close(ctx context.Context) error {
	err := nc.sub.Close()
	if err != nil {
		return err
	}
	return nil
}

type NatsProducer struct {
	topic string
	conn  stan.Conn
}

var _ Producer = (*NatsProducer)(nil)

func (np *NatsProducer) Produce(ctx context.Context, value []byte, opt *ProduceOptions) error {
	var aggregateID string
	if opt != nil {
		aggregateID = opt.AggregateID
	} else {
		aggregateID = uuid()
	}

	msg := &Message{
		ID:          uuid(),
		AggregateID: aggregateID,
		Value:       value,
	}

	data, err := msg.Encode()
	if err != nil {
		return err
	}

	return np.conn.Publish(np.topic, data)
}

type Nats struct {
	config *NatsConfig
	conn   stan.Conn
}

func (n *Nats) name() string {
	return fmt.Sprintf("%s.%s", n.config.ClusterID, n.config.ClientID)
}

func (n *Nats) durableName(topic string) string {
	return fmt.Sprintf("%s.%s", n.name(), topic)
}

type NatsProducerConfig struct {
	Topic string
}

func (n *Nats) Producer(config *NatsProducerConfig) *NatsProducer {
	return &NatsProducer{
		topic: config.Topic,
		conn:  n.conn,
	}
}

type NatsConsumerConfig struct {
	Topic   string
	Group   string
	Durable bool
}

func (n *Nats) Consumer(config *NatsConsumerConfig) (*NatsConsumer, error) {
	var err error

	options := []stan.SubscriptionOption{
		stan.SetManualAckMode(),
		stan.DeliverAllAvailable(),
	}

	if config.Durable {
		options = append(options, stan.DurableName(n.durableName(config.Topic)))
	}

	isGroupHandler := config.Group != ""
	messages := make(chan *Message, 1)

	prevID := ""

	handler := func(msg *stan.Msg) {
		message, err := MessageFromBytes(msg.Data)
		if err != nil {
			msg.Ack()
			return
		}

		if prevID == "" {
			prevID = message.ID
		} else if message.ID <= prevID {
			// this message has been been seen before
			// because id are lexical ordered
			msg.Ack()
			return
		}

		prevID = message.ID

		message.Ack = msg.Ack
		messages <- message
	}

	var sub stan.Subscription

	if isGroupHandler {
		sub, err = n.conn.QueueSubscribe(config.Topic, config.Group, handler, options...)
	} else {
		sub, err = n.conn.Subscribe(config.Topic, handler, options...)
	}

	if err != nil {
		return nil, err
	}

	return &NatsConsumer{
		messages: messages,
		sub:      sub,
	}, err
}

func (n *Nats) Open(ctx context.Context) error {
	natsOpts := make([]nats.Option, 0)
	if n.config.TLS != nil {
		natsOpts = append(natsOpts, nats.Secure(n.config.TLS))
	}

	nc, err := nats.Connect(n.config.Addr, natsOpts...)
	if err != nil {
		return err
	}

	for i := 0; i < 10; i++ {
		n.conn, err = stan.Connect(n.config.ClusterID, n.config.ClientID, stan.NatsConn(nc))
		if err != nil {
			if err == stan.ErrConnectReqTimeout {
				time.Sleep(1 * time.Second)
				continue
			}
			return err
		}

		return nil
	}

	return stan.ErrConnectReqTimeout
}

func (n *Nats) Close() error {
	return n.conn.Close()
}

type NatsConfig struct {
	ClientID  string
	ClusterID string
	Addr      string
	TLS       *tls.Config
}

func NewNats(config *NatsConfig) *Nats {
	return &Nats{config: config}
}
