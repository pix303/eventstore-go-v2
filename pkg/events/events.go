package events

import (
	"time"

	"github.com/beevik/guid"
)

type AggregateEvent interface {
	GetID() string
	GetAggregateID() string
	GetAggregateName() string
	GetEventType() string
}

type StoreEvent[T any] struct {
	EventType     string
	ID            string
	AggregateID   string
	AggregateName string
	CreatedAt     time.Time
	CreatedBy     string
	PayloadData   T
}

func NewStoreEvent[T any](eventType, aggregateName, userID string, payloadData T) StoreEvent[T] {
	se := StoreEvent[T]{
		EventType:     eventType,
		ID:            guid.New().String(),
		AggregateID:   guid.New().String(),
		AggregateName: aggregateName,
		CreatedAt:     time.Now(),
		CreatedBy:     userID,
		PayloadData:   payloadData,
	}

	return se
}

func (se StoreEvent[T]) GetID() string {
	return se.ID
}

func (se StoreEvent[T]) GetAggregateID() string {
	return se.AggregateID
}

func (se StoreEvent[T]) GetAggregateName() string {
	return se.AggregateName
}

func (se StoreEvent[T]) GetEventType() string {
	return se.EventType
}
