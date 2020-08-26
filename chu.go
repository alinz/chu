package chu

import (
	"context"
)

type Closer interface {
	Close(ctx context.Context) error
}

type Consumer interface {
	Consume(ctx context.Context) (*Message, error)
}

type ProduceOptions struct {
	AggregateID string
}

type Producer interface {
	Produce(ctx context.Context, value []byte, opt *ProduceOptions) error
}
