package events

import (
	"reflect"
	"time"

	"github.com/beevik/guid"
	"github.com/pix303/eventstore-go-v2/pkg/utils"
)

type StoreEvent struct {
	Id              int64
	EventType       string
	AggregateID     string
	AggregateName   string
	CreatedAt       *time.Time
	CreatedBy       string
	PayloadData     string
	PayloadDataType string
}

func NewStoreEvent[T any](eventType, aggregateName, userID string, payload T, aggregateID *string) (StoreEvent, error) {
	var fakeEvent StoreEvent
	payloadData, err := utils.EncodePayload(payload)
	if err != nil {
		return fakeEvent, err
	}

	payloadDataType := reflect.TypeOf(payload).String()

	finalAggregateID := guid.New().String()
	if aggregateID != nil {
		finalAggregateID = *aggregateID
	}

	now := time.Now().UTC()

	se := StoreEvent{
		EventType:       eventType,
		Id:              0,
		AggregateID:     finalAggregateID,
		AggregateName:   aggregateName,
		CreatedAt:       &now,
		CreatedBy:       userID,
		PayloadData:     payloadData,
		PayloadDataType: payloadDataType,
	}

	return se, nil
}

func (se StoreEvent) GetPayload() (payloadDataType string, payloadData string) {
	payloadData = se.PayloadData
	payloadDataType = se.PayloadDataType
	return
}
