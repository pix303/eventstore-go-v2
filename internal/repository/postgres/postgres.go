package postgres

import (
	_ "embed"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"github.com/pix303/eventstore-go-v2/pkg/events"
	"github.com/pix303/postgres-util-go/pkg/postgres"
)

var insertStmt string = `
INSERT INTO store.events(
    "aggregateid",
    "aggregatename",
    "createdby",
    "createdat",
    "eventtype",
    "payloaddata",
    "payloaddatatype"
)
VALUES(
	:aggregateId,
	:aggregateName,
	:createdBy,
	:createdAt,
	:eventType,
	:payloadData,
	:payloadDataType
);
	`

type PostgresRepository struct {
	DB *sqlx.DB
}

func NewPostgresqlRepository() (*PostgresRepository, error) {
	db, err := postgres.NewPostgresqlRepository()
	if err != nil {
		return nil, err
	}

	return &PostgresRepository{DB: db}, nil
}

func (repo *PostgresRepository) Append(event events.StoreEvent) (bool, error) {
	payloadDataType, payloadData := event.GetPayload()
	result, err := repo.DB.NamedExec(insertStmt, map[string]any{
		"aggregateId":     event.AggregateID,
		"aggregateName":   event.AggregateName,
		"createdBy":       event.CreatedBy,
		"createdAt":       event.CreatedAt,
		"eventType":       event.EventType,
		"payloadData":     payloadData,
		"payloadDataType": payloadDataType,
	})

	if err != nil {
		return false, err
	}

	numRow, err := result.RowsAffected()
	if err != nil {
		return false, err
	}

	if numRow == 0 {
		return false, postgres.ErrPostgresqlNoEventAppended
	}

	return true, nil
}

var selectByIdStmt string = `
SELECT * FROM store.events WHERE 'id' = :id
	`

func (repo *PostgresRepository) RetriveByID(id string) (*events.StoreEvent, bool, error) {
	var result events.StoreEvent
	err := repo.DB.Select(result, selectByIdStmt, map[string]any{"id": id})

	if err != nil {
		return nil, false, err
	}

	return &result, true, nil
}

var selectByAggregateIdStmt string = `
SELECT * FROM store.events WHERE aggregateid = $1
	`

func (repo *PostgresRepository) RetriveByAggregateID(id string) ([]events.StoreEvent, bool, error) {
	var result []events.StoreEvent
	err := repo.DB.Select(&result, selectByAggregateIdStmt, id)

	if err != nil {
		return nil, false, err
	}

	return result, len(result) > 0, nil
}

var selectByAggregateNameStmt string = `
SELECT * FROM store.events WHERE aggregatename = :name
	`

func (repo *PostgresRepository) RetriveByAggregateName(name string) ([]events.StoreEvent, bool, error) {
	var result []events.StoreEvent
	err := repo.DB.Select(result, selectByAggregateNameStmt, map[string]any{"name": name})

	if err != nil {
		return nil, false, err
	}

	return result, true, nil
}
