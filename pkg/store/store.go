package store

import (
	"github.com/pix303/eventstore-go-v2/internal/repository"
	"github.com/pix303/eventstore-go-v2/internal/repository/postgres"
	"github.com/pix303/eventstore-go-v2/pkg/broker"
	"github.com/pix303/eventstore-go-v2/pkg/events"
)

type EventStoreConfigurator func(store *EventStore) error

func WithInMemoryRepository(store *EventStore) error {
	store.Repository = &repository.InMemoryRepository{}

	return nil
}

func WithPostgresqlRepository(store *EventStore) error {
	pr, err := postgres.NewPostgresqlRepository()
	if err != nil {
		return err
	}
	store.Repository = pr
	return nil
}

type EventStore struct {
	repository.EventStoreRepositable
	IsProjectable    bool
	ProjectionBroker *broker.Broker
	ProjectionTopics []string
}

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

func (store *EventStore) Add(event events.StoreEvent) (bool, error) {
	result, err := store.Repository.Append(event)
	if err != nil {
		return false, err
	}

	if store.ProjectionBroker != nil {
		msg := broker.NewBrokerMessage(event.AggregateID, event.EventType, nil)
		for _, topic := range store.ProjectionTopics {
			store.ProjectionBroker.Publish(topic, msg)
		}
	}
	return result, err
}

func (store *EventStore) GetByName(aggregateName string) ([]events.StoreEvent, error) {
	result, ok, err := store.Repository.RetriveByAggregateName(aggregateName)
	if ok {
		return result, nil
	}
	return []events.StoreEvent{}, err
}

func (store *EventStore) GetByID(aggregateID string) ([]events.StoreEvent, error) {
	result, ok, err := store.Repository.RetriveByAggregateID(aggregateID)
	if ok {
		return result, nil
	}
	return []events.StoreEvent{}, err
}

func (store *EventStore) GetByEventID(ID string) (*events.StoreEvent, bool, error) {
	result, ok, err := store.Repository.RetriveByID(ID)
	if ok {
		return result, ok, nil
	}
	return nil, ok, err
}
