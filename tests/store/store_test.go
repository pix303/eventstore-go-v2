package store_test

import (
	"fmt"
	"os"
	"strconv"
	"testing"

	"github.com/pix303/eventstore-go-v2/pkg/events"
	"github.com/pix303/eventstore-go-v2/pkg/store"
)

func exitWithError(err error) {
	if err != nil {
		fmt.Println(fmt.Errorf("exit for %v", err))
		os.Exit(1)
	}
}

type FakePayload struct {
	data string
}

func TestStore_ok(t *testing.T) {
	store, err := store.NewEventStore([]store.EventStoreConfigurator{store.WithInMemoryRepository})
	exitWithError(err)
	payload := FakePayload{"hello"}
	evt, _ := events.NewStoreEvent("some-event", "something", "misterX", payload, nil)

	_, err = store.Add(evt)
	exitWithError(err)

	if evt.EventType != "some-event" {
		exitWithError(fmt.Errorf("expect some-event, got %s", evt.EventType))
	}

	_, err = store.Add(evt)
	_, err = store.Add(evt)
	exitWithError(err)

	result, err := store.GetByName("something")
	exitWithError(err)

	if len(result) != 3 {
		exitWithError(fmt.Errorf("expect 3 length, got %d", len(result)))
	}

	result, err = store.GetByID(evt.AggregateID)
	exitWithError(err)
	if len(result) != 3 {
		exitWithError(fmt.Errorf("expect 3 length, got %d", len(result)))
	}

	resultByID, ok, err := store.GetByEventID(strconv.Itoa(int(evt.Id)))
	exitWithError(err)

	if ok != true {
		exitWithError(fmt.Errorf("expect to be found , got %v", ok))
	}

	finalResultByID := *resultByID

	if finalResultByID.AggregateID != "something" {
		t.Errorf("event aggregate name must be something istead of %s", finalResultByID.AggregateID)
		exitWithError(fmt.Errorf("expect to be something, got %v", finalResultByID.AggregateName))
	}
}
