package actor_test

import (
	"testing"
	"time"

	"github.com/pix303/actor-lib/pkg/actor"
	"github.com/pix303/eventstore-go-v2/pkg/events"
	"github.com/pix303/eventstore-go-v2/pkg/store"
	"github.com/stretchr/testify/assert"
)

var testAddress = actor.NewAddress("local", "tester")

func TestEventStoreActor(t *testing.T) {

	eventStoreActor, err := store.NewEvenStoreActorWithInMemory()
	assert.NoError(t, err)
	actor.RegisterActor(&eventStoreActor)

	addStoreEvent, err := events.NewStoreEvent(
		"some-action-happened",
		"A123",
		"user1",
		"hello world",
		nil,
	)

	assert.NoError(t, err)

	returnBox := make(chan actor.Message)
	addMsg := actor.NewMessage(
		store.EventStoreAddress(),
		testAddress,
		store.AddEventBody{Event: addStoreEvent},
		returnBox,
	)

	err = actor.SendMessage(addMsg)
	assert.NoError(t, err)
	<-time.After(50 * time.Millisecond)

	select {
	case returnMsg := <-returnBox:
		assert.IsType(t, store.AddEventBodyResult{}, returnMsg.Body)
		if ckMsg, ok := returnMsg.Body.(store.AddEventBodyResult); ok {
			assert.True(t, ckMsg.Success)
		}
		break
	case <-time.After(1000 * time.Second):
		t.Error("return msg not received")
		break
	}

	checkMsgBody := store.CheckExistenceByAggregateIDBody{Id: addStoreEvent.AggregateID}
	checkMsg := actor.NewMessage(
		store.EventStoreAddress(),
		testAddress,
		checkMsgBody,
		returnBox,
	)

	err = actor.SendMessage(checkMsg)
	assert.NoError(t, err)

	select {
	case returnMsg := <-returnBox:
		assert.IsType(t, store.CheckExistenceByAggregateIDBodyResult{}, returnMsg.Body)
		if ckMsg, ok := returnMsg.Body.(store.CheckExistenceByAggregateIDBodyResult); ok {
			assert.True(t, ckMsg.Exists)
		}
		break
	case <-time.After(1000 * time.Second):
		t.Error("return msg not received")
		break
	}
}
