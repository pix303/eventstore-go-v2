package broker_test

import (
	"errors"
	"fmt"
	"os"
	"testing"

	"github.com/pix303/eventstore-go-v2/pkg/broker"
)

func exitWithError(err error) {
	if err != nil {
		fmt.Println(fmt.Errorf("exit for %v", err))
		os.Exit(1)
	}
}

func messageHandler(c chan broker.BrokerMessage) {
	for {
		fmt.Printf("this is the event msg that i just recived %s\n\n", <-c)
	}
}

func TestBrokerCreation_ok(t *testing.T) {
	topic := "test"
	b := broker.NewBroker()
	c := b.Subscribe(topic)
	b.Publish(topic, broker.NewBrokerMessage("123", "test-event", nil))
}
