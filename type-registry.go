package cqrs

import (
	"fmt"
	"reflect"
	"strings"
)

var methodHandlerPrefix = "Handle"

// HandlersCache is a map of types to functions that will be used to route event sourcing events
type HandlersCache map[reflect.Type]func(source interface{}, event interface{})

// TypeCache is a map of strings to reflect.Type structures
type TypeCache map[string]reflect.Type

// TypeRegistry providers a helper registry for mapping event types and handlers after performance json serializaton
type TypeRegistry interface {
	GetHandlers(interface{}) HandlersCache
	GetTypeByName(string) (reflect.Type, bool)
	RegisterAggregate(aggregate interface{}, events ...interface{})
	RegisterEvents(events ...interface{})
	RegisterType(interface{})
}

type defaultTypeRegistry struct {
	HandlersDirectory map[reflect.Type]HandlersCache
	Types             TypeCache
}

var cachedRegistry *defaultTypeRegistry

// NewTypeRegistry constructs a new TypeRegistry
func NewTypeRegistry() TypeRegistry {
	return newTypeRegistry()
}

func newTypeRegistry() *defaultTypeRegistry {
	if cachedRegistry == nil {
		handlersDirectory := make(map[reflect.Type]HandlersCache, 0)
		types := make(TypeCache, 0)

		cachedRegistry = &defaultTypeRegistry{handlersDirectory, types}
	}

	return cachedRegistry
}

func (r *defaultTypeRegistry) GetHandlers(source interface{}) HandlersCache {
	sourceType := reflect.TypeOf(source)
	var handlers HandlersCache

	handlerChan := make(chan bool)
	quit := make(chan struct{})

	defer close(quit)
	go func(handlers *HandlersCache) {
		select {
		case handlerChan <- internalGetHandlers(r, sourceType, source, handlers):
		//do nothing, this just blocks data race
		case <-quit:
			fmt.Println("quit")
		}
	}(&handlers)

	//wait for channel to do it's stuff
	<-handlerChan
	return handlers
}

func internalGetHandlers(r *defaultTypeRegistry, sourceType reflect.Type, source interface{}, handlers *HandlersCache) bool {
	if value, ok := r.HandlersDirectory[sourceType]; ok {
		*handlers = value
	} else {
		*handlers = createHandlersCache(source)
		r.HandlersDirectory[sourceType] = *handlers
	}

	return true
}

func (r *defaultTypeRegistry) GetTypeByName(typeName string) (reflect.Type, bool) {
	typeValue, ok := r.Types[typeName]
	return typeValue, ok
}

func (r *defaultTypeRegistry) RegisterType(source interface{}) {
	rawType := reflect.TypeOf(source)
	r.Types[rawType.String()] = rawType
	PackageLogger().Debugf("Type Registered - %s", rawType.String())
}

func (r *defaultTypeRegistry) RegisterAggregate(aggregate interface{}, events ...interface{}) {
	r.RegisterType(aggregate)

	r.RegisterEvents(events)
}

func (r *defaultTypeRegistry) RegisterEvents(events ...interface{}) {
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
			//   func (source *MySource) HandleMyEvent(e MyEvent).
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
