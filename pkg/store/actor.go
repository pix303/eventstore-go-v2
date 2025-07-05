package store

import (
	"errors"
	"log/slog"

	"github.com/pix303/actor-lib/pkg/actor"
	"github.com/pix303/eventstore-go-v2/internal/repository"
	"github.com/pix303/eventstore-go-v2/internal/repository/postgres"
	"github.com/pix303/eventstore-go-v2/pkg/events"
)

func EventStoreAddress() *actor.Address {
	return actor.NewAddress("local", "eventstore")
}

func NewEvenStoreActorWithInMemory() (actor.Actor, error) {
	s, err := NewEventStoreState([]EventStoreStateConfigurator{
		WithInMemoryRepositoryForActor,
	})

	if err != nil {
		return actor.Actor{}, err
	}

	return actor.NewActor(
		EventStoreAddress(),
		&s,
	)
}

func NewEvenStoreActorWithPostgres() (actor.Actor, error) {
	s, err := NewEventStoreState([]EventStoreStateConfigurator{
		WithPostgresqlRepositoryForActor,
	})

	if err != nil {
		return actor.Actor{}, err
	}

	return actor.NewActor(
		EventStoreAddress(),
		&s,
	)
}

type EventStoreState struct {
	repository.EventStoreRepositable
}

type EventStoreStateConfigurator func(state *EventStoreState) error

func WithInMemoryRepositoryForActor(state *EventStoreState) error {
	state.Repository = &repository.InMemoryRepository{}
	return nil
}

func WithPostgresqlRepositoryForActor(store *EventStoreState) error {
	pr, err := postgres.NewPostgresqlRepository()
	if err != nil {
		return err
	}
	store.Repository = pr
	return nil
}

func NewEventStoreState(configs []EventStoreStateConfigurator) (EventStoreState, error) {
	s := EventStoreState{}
	for _, c := range configs {
		err := c(&s)
		if err != nil {
			return s, err
		}
	}
	return s, nil
}

type AddEventBody struct {
	Event events.StoreEvent
}
type ResultAddEventBody struct {
	Success bool
}

type RetriveByAggregateNameBody struct {
	Name string
}
type RetriveByAggregateIDBody struct {
	Id string
}
type RetriveByAggregateBodyResult struct {
	Result []events.StoreEvent
}

type CheckExistenceByAggregateIDBody struct {
	Id string
}
type CheckExistenceByAggregateIDBodyResult struct {
	Exists bool
}

type RetriveByEventIDBody struct {
	Id string
}
type RetriveByEventIDBodyResult struct {
	Result events.StoreEvent
}

func (this *EventStoreState) Process(inbox chan actor.Message) {
	for {
		msg := <-inbox
		switch payload := msg.Body.(type) {
		case AddEventBody:
			slog.Info("it is an add message", slog.Any("p", payload))
			panic("not implemented")

		case RetriveByAggregateNameBody:
			slog.Info("it is an retrive message", slog.Any("p", payload))
			panic("not implemented")
		}
	}

}

func (this *EventStoreState) ProcessSync(msg actor.Message) (actor.Message, error) {
	switch payload := msg.Body.(type) {
	case AddEventBody:
		{
			result, err := this.Repository.Append(payload.Event)

			if err != nil {
				slog.Error("error on add event to store", slog.Any("err", err))
				return actor.EmptyMessage(), err
			}

			return actor.NewMessage(
				msg.From,
				msg.To,
				ResultAddEventBody{Success: result},
			), nil
		}

	case RetriveByAggregateIDBody:
		result, _, err := this.Repository.RetriveByAggregateID(payload.Id)

		if err != nil {
			slog.Error("error on retrive aggregate events from store", slog.Any("err", err))
			return actor.EmptyMessage(), err
		}

		return actor.NewMessage(
			msg.From,
			msg.To,
			RetriveByAggregateBodyResult{result},
		), err

	case CheckExistenceByAggregateIDBody:
		_, result, err := this.Repository.RetriveByAggregateID(payload.Id)

		if err != nil {
			slog.Error("error on retrive aggregate events from store", slog.Any("err", err))
			return actor.EmptyMessage(), err
		}

		return actor.NewMessage(
			msg.From,
			msg.To,
			CheckExistenceByAggregateIDBodyResult{result},
		), err

	case RetriveByAggregateNameBody:
		result, _, err := this.Repository.RetriveByAggregateName(payload.Name)
		if err != nil {
			slog.Error("error on retrive aggregate events from store", slog.Any("err", err))
			return actor.EmptyMessage(), err
		}
		return actor.NewMessage(
			nil,
			msg.To,
			RetriveByAggregateBodyResult{result},
		), err

	default:
		slog.Info("unknown message", slog.Any("p", payload))
		return actor.EmptyMessage(), errors.New("message not implemented")
	}

}

func (this *EventStoreState) Shutdown() {
	slog.Info("shutdown event store state")
}
