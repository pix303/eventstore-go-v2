package repository

import "github.com/pix303/eventstore-go-v2/pkg/events"

type EventStoreRepository interface {
	Append(event events.StoreEvent) (bool, error)
	RetriveByID(id string) (*events.StoreEvent, bool, error)
	RetriveByAggregateID(id string) ([]events.StoreEvent, bool, error)
	RetriveByAggregateName(name string) ([]events.StoreEvent, bool, error)
}

type EventStoreRepositable struct {
	Repository EventStoreRepository
}
