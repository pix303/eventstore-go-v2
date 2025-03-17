package utils_test

import (
	"log"
	"testing"

	"github.com/pix303/eventstore-go-v2/pkg/events"
	"github.com/pix303/eventstore-go-v2/pkg/utils"
	"github.com/stretchr/testify/assert"
)

type Person struct {
	Name     string
	Age      uint
	Children []Person
}

var testPerson = Person{
	Name: "Mino",
	Age:  45,
	Children: []Person{
		{Name: "Nino", Age: 12},
		{Name: "Ninetto", Age: 1},
	},
}

func TestEncodeDecodePerson_success(t *testing.T) {
	ps, err := utils.EncodePayload(testPerson)
	assert.Nil(t, err)
	assert.Greater(t, len(ps), 0)

	p2, err := utils.DecodePayload[Person](ps)

	assert.Nil(t, err)
	assert.Equal(t, p2.Name, "Mino")
}

func TestEncodeDecodeEvent_success_fail(t *testing.T) {
	aggId := "abc123"
	evt, err := events.NewStoreEvent("type1", "aggregate1", "me", testPerson, &aggId)
	if err != nil {
		t.Errorf("error %v", err)
	}

	ps, err := utils.EncodePayload(evt)
	assert.Nil(t, err)
	assert.Greater(t, len(ps), 0)

	decodedEvt, err := utils.DecodePayload[events.StoreEvent](ps)

	assert.Nil(t, err)
	assert.Equal(t, decodedEvt.AggregateID, "abc123")
	assert.Equal(t, decodedEvt.AggregateName, "aggregate1")
	assert.Equal(t, decodedEvt.EventType, "type1")
	assert.Equal(t, decodedEvt.CreatedBy, "me")

	_, err = utils.DecodePayload[Person](ps)
	assert.NotNil(t, err)

}
