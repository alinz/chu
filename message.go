package chu

import (
	"github.com/alinz/chu.go/internal/binary"
)

type Message struct {
	ID          string
	AggregateID string
	Value       []byte
	Ack         func() error
}

func (m *Message) Encode() ([]byte, error) {
	size := len(m.ID) + 8
	size += len(m.AggregateID) + 8
	size += len(m.Value) + 8

	bin := binary.NewEncoding(size)

	err := bin.EncodeString(m.ID)
	if err != nil {
		return nil, err
	}

	err = bin.EncodeString(m.AggregateID)
	if err != nil {
		return nil, err
	}

	err = bin.EncodeBytes(m.Value)
	if err != nil {
		return nil, err
	}

	return bin.Bytes(), nil
}

func (m *Message) Decode(b []byte) error {
	var err error

	bin := binary.NewDecoding(b)

	m.ID, err = bin.DecodeString()
	if err != nil {
		return err
	}

	m.AggregateID, err = bin.DecodeString()
	if err != nil {
		return err
	}

	m.Value, err = bin.DecodeBytes()
	if err != nil {
		return err
	}

	return nil
}

func MessageFromBytes(data []byte) (*Message, error) {
	msg := &Message{}
	err := msg.Decode(data)
	if err != nil {
		return nil, err
	}
	return msg, nil
}
