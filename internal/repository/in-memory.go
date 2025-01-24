package repository

import (
	"github.com/pix303/eventstore-go-v2/pkg/errors"
	"github.com/pix303/eventstore-go-v2/pkg/events"
)

type InMemoryRepository struct {
	events []events.AggregateEvent
}

func (repo *InMemoryRepository) Append(event events.AggregateEvent) (bool, error) {
	repo.events = append(repo.events, event)
	return true, nil
}

func (repo *InMemoryRepository) RetriveByID(id string) (*events.AggregateEvent, bool, error) {
	for _, evt := range repo.events {
		if evt.GetID() == id {
			return &evt, true, nil
		}
	}
	return nil, false, errors.NotFoundAggregateID
}

func (repo *InMemoryRepository) RetriveByAggregateID(id string) ([]events.AggregateEvent, bool, error) {
	result := []events.AggregateEvent{}
	for _, evt := range repo.events {
		if evt.GetAggregateID() == id {
			result = append(result, evt)
		}
	}

	if len(result) > 0 {
		return result, true, nil
	}
	return []events.AggregateEvent{}, false, errors.NotFoundAggregateID
}

func (repo *InMemoryRepository) RetriveByAggregateName(name string) ([]events.AggregateEvent, bool, error) {
	result := []events.AggregateEvent{}
	for _, evt := range repo.events {
		if evt.GetAggregateName() == name {
			result = append(result, evt)
		}
	}

	if len(result) > 0 {
		return result, true, nil
	}
	return []events.AggregateEvent{}, false, errors.NotFoundAggregateID
}
