package repository

import (
	"strconv"

	"github.com/pix303/eventstore-go-v2/pkg/errors"
	"github.com/pix303/eventstore-go-v2/pkg/events"
)

type InMemoryRepository struct {
	events []events.StoreEvent
}

func (repo *InMemoryRepository) Append(event events.StoreEvent) (bool, error) {
	repo.events = append(repo.events, event)
	return true, nil
}

func (repo *InMemoryRepository) RetriveByID(id string) (*events.StoreEvent, bool, error) {
	for _, evt := range repo.events {
		evtId := strconv.Itoa(int(evt.Id))
		if evtId == id {
			return &evt, true, nil
		}
	}
	return nil, false, errors.ErrNotFoundAggregateID
}

func (repo *InMemoryRepository) RetriveByAggregateID(id string) ([]events.StoreEvent, bool, error) {
	result := []events.StoreEvent{}
	for _, evt := range repo.events {
		if evt.AggregateID == id {
			result = append(result, evt)
		}
	}

	if len(result) > 0 {
		return result, true, nil
	}
	return []events.StoreEvent{}, false, errors.ErrNotFoundAggregateID
}

func (repo *InMemoryRepository) RetriveByAggregateName(name string) ([]events.StoreEvent, bool, error) {
	result := []events.StoreEvent{}
	for _, evt := range repo.events {
		if evt.AggregateID == name {
			result = append(result, evt)
		}
	}

	if len(result) > 0 {
		return result, true, nil
	}
	return []events.StoreEvent{}, false, errors.ErrNotFoundAggregateID
}
