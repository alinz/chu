package chu

import (
	"context"
)

type Closer interface {
	Close() error
}

type Consumer func(*Message)

type ProduceOptions struct {
	AggregateID string
}

type Producer interface {
	Produce(ctx context.Context, value []byte, opt *ProduceOptions) error
}
