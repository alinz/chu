package chu_test

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/nats-io/nats-streaming-server/server"
	"github.com/nats-io/nats.go"

	"github.com/alinz/chu.go"
)

const (
	testClusterName = "dummy_server"
	testTopic       = "root.a.b.c.test2"
)

func TestMain(m *testing.M) {
	natsStreamServer, err := server.RunServer(testClusterName)
	if err != nil {
		panic(err)
	}

	defer natsStreamServer.Shutdown()
	exitCode := m.Run()

	fmt.Printf("exit code: %d\n", exitCode)
}

func TestNats(t *testing.T) {
	broker := chu.NewNats(&chu.NatsConfig{
		Addr:      nats.DefaultURL,
		ClusterID: testClusterName,
		ClientID:  "foo",
	})

	err := broker.Open(context.Background())
	if err != nil {
		t.Error(err)
		return
	}

	defer broker.Close()

	producer := broker.Producer(&chu.NatsProducerConfig{
		Topic: testTopic,
	})

	data := []byte("hello world")

	err = producer.Produce(context.Background(), data, nil)
	if err != nil {
		t.Error(err)
		return
	}

	var wg sync.WaitGroup
	var msg *chu.Message

	wg.Add(1)

	consumer, err := broker.Consumer(&chu.NatsConsumerConfig{
		Topic: testTopic,
	}, func(m *chu.Message) {
		defer wg.Done()
		msg = m
	})
	if err != nil {
		t.Error(err)
		return
	}

	defer consumer.Close()

	wg.Wait()

	if !bytes.Equal(data, msg.Value) {
		t.Error("data is not correct")
	}
}
