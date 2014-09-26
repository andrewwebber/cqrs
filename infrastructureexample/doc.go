// Package infrastructureexample provides the example scenario utilizing couchbase and rabbitmq infastructure components
//
//
//  func TestEventSourcingWithCouchbase(t *testing.T) {
//    persistance, error := couchbase.NewEventStreamRepository("http://localhost:8091/")
//    if error != nil {
//      t.Fatal(error)
//    }
//
//    RunScenario(t, persistance)
//  }
//
//  func RunScenario(t *testing.T, persistance cqrs.EventStreamRepository) {
//    bus := rabbit.NewEventBus("amqp://guest:guest@localhost:5672/", "example_test", "testing.example")
//    repository := cqrs.NewRepositoryWithPublisher(persistance, bus)
//    ...
//  }
package infrastructureexample
