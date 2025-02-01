package store

import (
	"github.com/pix303/eventstore-go-v2/internal/repository"
	"github.com/pix303/eventstore-go-v2/pkg/broker"
	"github.com/pix303/eventstore-go-v2/pkg/events"
)

type EventStoreRepository interface {
	Append(event events.AggregateEvent) (bool, error)
	RetriveByID(id string) (*events.AggregateEvent, bool, error)
	RetriveByAggregateID(id string) ([]events.AggregateEvent, bool, error)
	RetriveByAggregateName(name string) ([]events.AggregateEvent, bool, error)
}

type EventStore struct {
	Repository       EventStoreRepository
	IsProjectable    bool
	ProjectionBroker *broker.Broker
	ProjectionTopics []string
}

type EventStoreConfigurator func(store *EventStore) error

func NewEventStore(configures []EventStoreConfigurator) (EventStore, error) {
	store := EventStore{}
	for _, c := range configures {
		err := c(&store)
		if err != nil {
			return store, err
		}
	}
	return store, nil
}

func WithInMemoryRepository(store *EventStore) error {
	store.Repository = &repository.InMemoryRepository{}
	return nil
}

type ProjectionChannelHandler func(c chan broker.BrokerMessage, store *EventStore)

func NewProjectionHandlersConfig(projectionHandlers map[string]ProjectionChannelHandler) EventStoreConfigurator {
	return func(store *EventStore) error {
		store.ProjectionBroker = broker.NewBroker()
		for key, handler := range projectionHandlers {
			c := make(chan broker.BrokerMessage)
			store.ProjectionBroker.SubscribeWithChan(key, c)
			store.ProjectionTopics = append(store.ProjectionTopics, key)
			go handler(c, store)
		}
		return nil
	}
}

func (store *EventStore) Add(event events.AggregateEvent) (bool, error) {
	result, err := store.Repository.Append(event)
	if err != nil {
		return false, err
	}

	if store.ProjectionBroker != nil {
		msg := broker.NewBrokerMessage(event.GetAggregateID(), event.GetEventType(), nil)
		for _, topic := range store.ProjectionTopics {
			store.ProjectionBroker.Publish(topic, msg)
		}
	}
	return result, err
}

func (store *EventStore) GetByName(aggregateName string) ([]events.AggregateEvent, error) {
	result, ok, err := store.Repository.RetriveByAggregateName(aggregateName)
	if ok {
		return result, nil
	}
	return []events.AggregateEvent{}, err
}

func (store *EventStore) GetByID(aggregateID string) ([]events.AggregateEvent, error) {
	result, ok, err := store.Repository.RetriveByAggregateID(aggregateID)
	if ok {
		return result, nil
	}
	return []events.AggregateEvent{}, err
}

func (store *EventStore) GetByEventID(ID string) (*events.AggregateEvent, bool, error) {
	result, ok, err := store.Repository.RetriveByID(ID)
	if ok {
		return result, ok, nil
	}
	return nil, ok, err
}
