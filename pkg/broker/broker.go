package broker

import (
	"sync"
)

type Broker struct {
	topicChannels map[string][]chan BrokerMessage
	closed        bool
	guard         sync.Mutex
}

func NewBroker() *Broker {
	return &Broker{
		make(map[string][]chan BrokerMessage),
		false,
		sync.Mutex{},
	}
}

func (b *Broker) Subscribe(topic string) chan BrokerMessage {
	b.guard.Lock()
	defer b.guard.Unlock()

	var c = make(chan BrokerMessage)
	channels := b.topicChannels[topic]
	channels = append(channels, c)
	b.topicChannels[topic] = channels
	return c
}

func (b *Broker) SubscribeWithChan(topic string, c chan BrokerMessage) {
	b.guard.Lock()
	defer b.guard.Unlock()

	channels := b.topicChannels[topic]
	channels = append(channels, c)
	b.topicChannels[topic] = channels
}

func (b *Broker) Unsubscribe(topic string) {
	b.guard.Lock()
	defer b.guard.Unlock()

	for _, c := range b.topicChannels[topic] {
		close(c)
	}

	delete(b.topicChannels, topic)
}

func (b *Broker) Publish(topic string, message BrokerMessage) {
	b.guard.Lock()
	defer b.guard.Unlock()

	channels := b.topicChannels[topic]
	for _, c := range channels {
		c <- message
	}
}

func (b *Broker) Close() {
	if b.closed == true {
		return
	}

	for k := range b.topicChannels {
		b.Unsubscribe(k)
	}
}

type BrokerMessage struct {
	AggregateID string
	EventType   string
	Payload     string
}

func NewBrokerMessage(id, eventType string, payload *string) BrokerMessage {
	finalPayload := ""
	if payload != nil {
		finalPayload = *payload
	}
	return BrokerMessage{
		id,
		eventType,
		finalPayload,
	}
}
