package postgres

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/jmoiron/sqlx/types"
	_ "github.com/lib/pq"

	"github.com/andrewwebber/cqrs"
)

const (
	eventsTable            = "events"
	eventsIntegrationTable = "events_integration"
	eventsCorrelationTable = "events_correlation"
)

var (
	ErrPgConnStr             = errors.New("invalid postgres connection string")
	ErrPgPrepInsertStmt      = errors.New("error prepapring insert statement")
	ErrPgPrepSelectStmt      = errors.New("error preparing select statement")
	ErrPgSelectSource        = errors.New("error querying for events using source id")
	ErrPgNoEventType         = errors.New("cannot find event type")
	ErrPgUnmarshalingEvent   = errors.New("error unmarshaling event")
	ErrPgMarshalingEvent     = errors.New("error marshaling event")
	ErrPgSavingEvent         = errors.New("error saving event")
	ErrPgSavingIntegEvent    = errors.New("error saving integration event")
	ErrPgSavingCorrelEvent   = errors.New("error saving correlation event")
	ErrPgIntegEventsByCorrel = errors.New("error querying for integration events using correlation id")
	ErrPgAllIntegEvents      = errors.New("error querying for all integration events")
)

type pgProviderError struct {
	Value     interface{}
	Err       error
	ActualErr error
}

func (pgpe pgProviderError) Error() string {
	if pgpe.Value != nil {
		return fmt.Sprintf("%s: %+v\n%v", pgpe.Err, pgpe.Value, pgpe.ActualErr)
	} else {
		return fmt.Sprintf("%s:\n%v", pgpe.Err, pgpe.ActualErr)
	}
}

type PgVersionedEvent struct {
	ID            string         `db:"id"`
	CorrelationID string         `db:"correlationid"`
	SourceID      string         `db:"sourceid"`
	Version       int            `db:"version"`
	EventType     string         `db:"eventtype"`
	Created       time.Time      `db:"created"`
	Event         types.JsonText `db:"event"`
}

type EventStreamRepository struct {
	db                    *sqlx.DB
	insertEventStmt       *sqlx.NamedStmt
	insertIntegEventStmt  *sqlx.NamedStmt
	insertCorrelEventStmt *sqlx.NamedStmt
	selectEventStmt       *sqlx.NamedStmt
	typeRegistry          cqrs.TypeRegistry
}

func NewEventStreamRepository(connStr string, typeRegistry cqrs.TypeRegistry) (*EventStreamRepository, error) {
	db, err := sqlx.Connect("postgres", connStr)
	if err != nil {
		return nil, pgProviderError{connStr, ErrPgConnStr, err}
	}

	repository := &EventStreamRepository{
		db:           db,
		typeRegistry: typeRegistry,
	}

	tables := []string{eventsTable, eventsIntegrationTable, eventsCorrelationTable}
	for _, table := range tables {
		sql := fmt.Sprintf(`
			INSERT INTO %s(id, correlationid, sourceid, version, eventtype, created, event) 
			VALUES (:id, :correlationid, :sourceid, :version, :eventtype, :created, :event)
		`, table)
		stmt, err := db.PrepareNamed(sql)
		if err != nil {
			return nil, pgProviderError{table, ErrPgPrepInsertStmt, err}
		}
		switch table {
		case eventsTable:
			repository.insertEventStmt = stmt
			break
		case eventsIntegrationTable:
			repository.insertIntegEventStmt = stmt
			break
		case eventsCorrelationTable:
			repository.insertCorrelEventStmt = stmt
			break
		}
	}

	repository.selectEventStmt, err = db.PrepareNamed("SELECT * FROM events WHERE sourceid = :sourceid")
	if err != nil {
		return nil, pgProviderError{nil, ErrPgPrepSelectStmt, err}
	}
	return repository, nil
}

func (esr *EventStreamRepository) Get(id string) ([]cqrs.VersionedEvent, error) {
	pgevents := []PgVersionedEvent{}
	param := struct{ SourceID string }{SourceID: id}
	if err := esr.selectEventStmt.Select(&pgevents, param); err != nil {
		return nil, pgProviderError{id, ErrPgSelectSource, err}
	}

	var events []cqrs.VersionedEvent
	for _, pgevent := range pgevents {
		event, err := esr.fromPgEvent(pgevent)
		if err != nil {
			return nil, err
		}
		events = append(events, *event)
	}
	return events, nil
}

func (esr *EventStreamRepository) Save(sourceID string, events []cqrs.VersionedEvent) error {
	for _, event := range events {
		pgevent, err := esr.toPgEvent(event)
		if err != nil {
			return err
		}

		// call private saveIntegration, to avoid duplicate toPgEvent conversion
		if err := esr.saveIntegration(pgevent); err != nil {
			return err
		}
		if _, err := esr.insertEventStmt.Exec(pgevent); err != nil {
			return pgProviderError{pgevent, ErrPgSavingEvent, err}
		}
	}
	return nil
}

func (esr *EventStreamRepository) SaveIntegrationEvent(event cqrs.VersionedEvent) error {
	pgevent, err := esr.toPgEvent(event)
	if err != nil {
		return err
	}
	return esr.saveIntegration(pgevent)
}

func (esr *EventStreamRepository) GetIntegrationEventsByCorrelationID(correlationID string) ([]cqrs.VersionedEvent, error) {
	sql := fmt.Sprintf("SELECT * FROM %s WHERE correlationid = $1 ORDER BY created ASC", eventsCorrelationTable)
	var pgevents []PgVersionedEvent
	if err := esr.db.Select(&pgevents, sql, correlationID); err != nil {
		return nil, pgProviderError{nil, ErrPgIntegEventsByCorrel, err}
	}

	var events []cqrs.VersionedEvent
	for _, pgevent := range pgevents {
		event, err := esr.fromPgEvent(pgevent)
		if err != nil {
			return nil, err
		}
		events = append(events, *event)
	}
	return events, nil
}

func (esr *EventStreamRepository) AllIntegrationEventsEverPublished() ([]cqrs.VersionedEvent, error) {
	sql := fmt.Sprintf("SELECT * FROM %s ORDER BY created ASC", eventsIntegrationTable)
	var pgevents []PgVersionedEvent
	if err := esr.db.Select(&pgevents, sql); err != nil {
		return nil, pgProviderError{nil, ErrPgAllIntegEvents, err}
	}

	var events []cqrs.VersionedEvent
	for _, pgevent := range pgevents {
		event, err := esr.fromPgEvent(pgevent)
		if err != nil {
			return nil, err
		}
		events = append(events, *event)
	}
	return events, nil
}

func (esr *EventStreamRepository) GetDb() *sqlx.DB {
	return esr.db
}

func (esr *EventStreamRepository) saveIntegration(pgevent *PgVersionedEvent) error {
	if _, err := esr.insertIntegEventStmt.Exec(pgevent); err != nil {
		return pgProviderError{pgevent, ErrPgSavingIntegEvent, err}
	}
	if _, err := esr.insertCorrelEventStmt.Exec(pgevent); err != nil {
		return pgProviderError{pgevent, ErrPgSavingCorrelEvent, err}
	}
	return nil
}

func (esr *EventStreamRepository) toPgEvent(event cqrs.VersionedEvent) (*PgVersionedEvent, error) {
	b, err := json.Marshal(event.Event)
	if err != nil {
		return nil, pgProviderError{event.Event, ErrPgMarshalingEvent, err}
	}

	return &PgVersionedEvent{
		ID:            event.ID,
		CorrelationID: event.CorrelationID,
		SourceID:      event.SourceID,
		Version:       event.Version,
		EventType:     event.EventType,
		Created:       event.Created,
		Event:         types.JsonText(b),
	}, nil
}

func (esr *EventStreamRepository) fromPgEvent(pgevent PgVersionedEvent) (*cqrs.VersionedEvent, error) {
	eventType, ok := esr.typeRegistry.GetTypeByName(pgevent.EventType)
	if !ok {
		return nil, pgProviderError{pgevent.EventType, ErrPgNoEventType, nil}
	}

	eventValue := reflect.New(eventType)
	e := eventValue.Interface()
	if err := json.Unmarshal(pgevent.Event, e); err != nil {
		return nil, pgProviderError{e, ErrPgUnmarshalingEvent, err}
	}
	return &cqrs.VersionedEvent{
		ID:        pgevent.ID,
		SourceID:  pgevent.SourceID,
		Version:   pgevent.Version,
		EventType: pgevent.EventType,
		Created:   pgevent.Created,
		Event:     reflect.Indirect(reflect.ValueOf(e)).Interface(),
	}, nil
}
