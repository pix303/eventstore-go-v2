package store

import (
	"log/slog"

	"github.com/pix303/cinecity/pkg/actor"
	"github.com/pix303/cinecity/pkg/subscriber"
	"github.com/pix303/eventstore-go-v2/internal/repository"
	"github.com/pix303/eventstore-go-v2/internal/repository/postgres"
	"github.com/pix303/eventstore-go-v2/pkg/events"
)

var EventStoreAddress = actor.NewAddress("local", "eventstore")

func NewEvenStoreActorWithInMemory() (actor.Actor, error) {
	s, err := NewEventStoreState([]EventStoreStateConfigurator{
		WithInMemoryRepositoryForActor,
	})

	if err != nil {
		return actor.Actor{}, err
	}

	return actor.NewActor(
		EventStoreAddress,
		s,
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
		EventStoreAddress,
		s,
	)
}

type EventStoreState struct {
	repository.EventStoreRepositable
	SubscriptionState *subscriber.SubscriptionsState
}

type EventStoreStateConfigurator func(state *EventStoreState) error

func WithInMemoryRepositoryForActor(state *EventStoreState) error {
	state.Repository = &repository.InMemoryRepository{}
	state.SubscriptionState = subscriber.NewSubscribeState()
	return nil
}

func WithPostgresqlRepositoryForActor(state *EventStoreState) error {
	pr, err := postgres.NewPostgresqlRepository()
	if err != nil {
		return err
	}
	state.Repository = pr
	state.SubscriptionState = subscriber.NewSubscribeState()
	return nil
}

func NewEventStoreState(configs []EventStoreStateConfigurator) (*EventStoreState, error) {
	s := EventStoreState{}
	for _, c := range configs {
		err := c(&s)
		if err != nil {
			return &s, err
		}
	}
	return &s, nil
}

type AddEventBody struct {
	Event events.StoreEvent
}
type AddEventBodyResult struct {
	Success bool
}

type StoreEventAddedBody struct {
	AggregateID string
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

func (state *EventStoreState) Process(msg actor.Message) {
	switch payload := msg.Body.(type) {
	case actor.AddSubscriptionMessageBody:
		slog.Debug("add subscription", slog.String("from", msg.From.String()))
		state.SubscriptionState.AddSubscription(msg.From)

	case actor.RemoveSubscriptionMessageBody:
		slog.Debug("remove subscription", slog.String("from", msg.From.String()))
		state.SubscriptionState.RemoveSubscription(msg.From)

	case AddEventBody:
		result, err := state.Repository.Append(payload.Event)
		if err != nil {
			slog.Warn("store add event error", slog.String("err", err.Error()))
		}

		if msg.WithReturn {
			resultMsg := actor.NewReturnMessage(AddEventBodyResult{Success: result}, msg)
			msg.ReturnChan <- actor.NewWrappedMessage(&resultMsg, err)
		}

		addDoneMsg := actor.NewSubscribersMessage(
			EventStoreAddress,
			StoreEventAddedBody{AggregateID: payload.Event.AggregateID},
		)
		state.SubscriptionState.NotifySubscribers(addDoneMsg)

	case CheckExistenceByAggregateIDBody:
		_, result, err := state.Repository.RetriveByAggregateID(payload.Id)
		if err != nil {
			slog.Warn("error on check existence aggregate events from store", slog.String("err", err.Error()))
		}
		if msg.WithReturn {
			returnMsg := actor.NewReturnMessage(CheckExistenceByAggregateIDBodyResult{result}, msg)
			msg.ReturnChan <- actor.NewWrappedMessage(&returnMsg, err)
		}

	case RetriveByAggregateNameBody:
		result, _, err := state.Repository.RetriveByAggregateName(payload.Name)
		if err != nil {
			slog.Warn("error on retrive by name error", slog.String("err", err.Error()))
		}
		if msg.WithReturn {
			returnMsg := actor.NewReturnMessage(RetriveByAggregateBodyResult{result}, msg)
			msg.ReturnChan <- actor.NewWrappedMessage(&returnMsg, err)
		}

	case RetriveByAggregateIDBody:
		result, _, err := state.Repository.RetriveByAggregateID(payload.Id)
		if err != nil {
			slog.Warn("error on retrive by ID error", slog.String("err", err.Error()))
		}

		if msg.WithReturn {
			returnMsg := actor.NewReturnMessage(RetriveByAggregateBodyResult{result}, msg)
			msg.ReturnChan <- actor.NewWrappedMessage(&returnMsg, err)
		}
	}
}

func (state *EventStoreState) GetState() any {
	return nil
}

func (state *EventStoreState) Shutdown() {
	state.Repository = nil
	state.SubscriptionState = nil
	slog.Info("eventstore state cleaned")
}
