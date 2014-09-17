package cqrs

import (
	"reflect"
	"strings"
)

var methodHandlerPrefix = "Handle"

// HandlersCache is a map of types to functions that will be used to route event sourcing events
type HandlersCache map[reflect.Type]func(source interface{}, event interface{})

// TypeRegistry providers a helper registry for mapping event types and handlers after performance json serializaton
type TypeRegistry interface {
	GetHandlers(interface{}) HandlersCache
	GetEventType(string) (reflect.Type, bool)
	RegisterAggregate(aggregate interface{}, events ...interface{})
}

type defaultTypeRegistry struct {
	handlersDirectory map[reflect.Type]HandlersCache
	eventTypes        map[string]reflect.Type
}

func newTypeRegistry() defaultTypeRegistry {
	handlersDirectory := make(map[reflect.Type]HandlersCache, 0)
	eventTypes := make(map[string]reflect.Type, 0)

	return defaultTypeRegistry{handlersDirectory, eventTypes}
}

func (r defaultTypeRegistry) GetHandlers(source interface{}) HandlersCache {
	sourceType := reflect.TypeOf(source)
	var handlers HandlersCache
	if value, ok := r.handlersDirectory[sourceType]; ok {
		handlers = value
	} else {
		handlers = createHandlersCache(source)

		r.handlersDirectory[sourceType] = handlers
	}

	return handlers
}

func (r defaultTypeRegistry) GetEventType(eventType string) (reflect.Type, bool) {
	if eventTypeValue, ok := r.eventTypes[eventType]; ok {
		return eventTypeValue, ok
	}

	return nil, false
}

func (r defaultTypeRegistry) RegisterType(source interface{}) {
	rawType := reflect.TypeOf(source)
	r.eventTypes[rawType.String()] = rawType
}

func (r defaultTypeRegistry) RegisterAggregate(aggregate interface{}, events ...interface{}) {
	r.RegisterType(aggregate)

	for _, event := range events {
		r.RegisterType(event)
	}
}

func createHandlersCache(source interface{}) HandlersCache {
	sourceType := reflect.TypeOf(source)
	handlers := make(HandlersCache)

	methodCount := sourceType.NumMethod()
	for i := 0; i < methodCount; i++ {
		method := sourceType.Method(i)

		if strings.HasPrefix(method.Name, methodHandlerPrefix) {
			//   func (source *MySource) HandleMyEvent(e *MyEvent).
			if method.Type.NumIn() == 2 {
				eventType := method.Type.In(1)
				handler := func(source interface{}, event interface{}) {
					sourceValue := reflect.ValueOf(source)
					eventValue := reflect.ValueOf(event)

					method.Func.Call([]reflect.Value{sourceValue, eventValue})
				}

				handlers[eventType] = handler
			}
		}
	}

	return handlers
}
