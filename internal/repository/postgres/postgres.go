package postgres

import (
	_ "embed"
	"fmt"
	"os"
	"strconv"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"github.com/pix303/eventstore-go-v2/pkg/events"
	"github.com/pix303/postgres-util-go/pkg/postgres"
)

type PostgresConnctionInfo struct {
	Host   string
	Port   int
	User   string
	Pass   string
	DBname string
}

type PostgresConnctionInfoBuilder struct {
	info PostgresConnctionInfo
	errs []error
}

func (builder *PostgresConnctionInfoBuilder) WithHost() *PostgresConnctionInfoBuilder {
	pgHost := os.Getenv("PG_HOST")
	if pgHost != "" {
		builder.info.Host = pgHost
	} else {
		builder.errs = append(builder.errs, postgres.PostgresqlConfigNoHostError)
	}
	pgPort := os.Getenv("PG_PORT")
	if pgPort != "" {
		pgPortInt, err := strconv.Atoi(pgPort)
		if err != nil {
			builder.errs = append(builder.errs, err)
		}
		builder.info.Port = pgPortInt
	} else {
		builder.errs = append(builder.errs, postgres.PostgresqlConfigNoPortError)
	}
	return builder
}

func (builder *PostgresConnctionInfoBuilder) WithUserAndPass() *PostgresConnctionInfoBuilder {
	pgUser := os.Getenv("PG_USER")
	if pgUser != "" {
		builder.info.User = pgUser
	} else {
		builder.errs = append(builder.errs, postgres.PostgresqlConfigNoUserError)
	}
	pgPass := os.Getenv("PG_PASS")
	if pgPass != "" {
		builder.info.Pass = pgPass
	} else {
		builder.errs = append(builder.errs, postgres.PostgresqlConfigNoPasswordError)
	}
	return builder
}

func (builder *PostgresConnctionInfoBuilder) WithDBName() *PostgresConnctionInfoBuilder {
	pgDBName := os.Getenv("PG_DBNAME")
	if pgDBName != "" {
		builder.info.DBname = pgDBName
	} else {
		builder.errs = append(builder.errs, postgres.PostgresqlConfigNoDBNameError)
	}
	return builder
}

func (builder *PostgresConnctionInfoBuilder) Build() (PostgresConnctionInfo, error) {
	if len(builder.errs) > 0 {
		return PostgresConnctionInfo{}, builder.errs[0]
	}
	return builder.info, nil
}

func NewPostgresqlRepository() (*PostgresRepository, error) {
	pcib := PostgresConnctionInfoBuilder{}

	connectionInfo, err := pcib.WithHost().WithUserAndPass().WithDBName().Build()
	if err != nil {
		return nil, err
	}
	connStr := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		connectionInfo.Host,
		connectionInfo.Port,
		connectionInfo.User,
		connectionInfo.Pass,
		connectionInfo.DBname,
	)

	db, err := sqlx.Connect("postgres", connStr)
	if err != nil {
		return nil, err
	}

	err = db.Ping()
	if err != nil {
		return nil, err
	}

	pr := PostgresRepository{
		DB: db,
	}

	return &pr, nil
}

type PostgresRepository struct {
	DB *sqlx.DB
}

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
		return false, postgres.PostgresqlNoEventAppendedError
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

	return result, true, nil
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
