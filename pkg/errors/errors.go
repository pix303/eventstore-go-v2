package errors

import (
	"errors"
)

var (
	NotFoundAggregateID             = errors.New("not found")
	PostgresqlConfigNoHostError     = errors.New("no postgres host config")
	PostgresqlConfigNoPortError     = errors.New("no postgres host port config")
	PostgresqlConfigNoUserError     = errors.New("no postgres username")
	PostgresqlConfigNoPasswordError = errors.New("no postgres password")
	PostgresqlConfigNoDBNameError   = errors.New("no postgres db name config")
	PostgresqlNoEventAppendedError  = errors.New("no event was appended")
)
