package cqrs

import "github.com/prometheus/client_golang/prometheus"

var (
	metricsCommandsDispatched *prometheus.CounterVec
	metricsCommandsFailed     *prometheus.CounterVec
	metricsEventsDispatched   *prometheus.CounterVec
	metricsEventsFailed       *prometheus.CounterVec
)

func init() {
	metricsCommandsDispatched = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name:      "cqrs_commands_dispatched",
		Subsystem: "ix",
		Help:      "CQRS Commands Dispatched",
	}, []string{"command"})

	metricsCommandsFailed = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name:      "cqrs_commands_failed",
		Subsystem: "ix",
		Help:      "CQRS Commands Failed",
	}, []string{"command"})

	metricsEventsDispatched = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name:      "cqrs_events_dispatched",
		Subsystem: "ix",
		Help:      "CQRS Events Dispatched",
	}, []string{"event"})

	metricsEventsFailed = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name:      "cqrs_events_failed",
		Subsystem: "ix",
		Help:      "CQRS Events Failed",
	}, []string{"event"})

	prometheus.MustRegister(metricsCommandsDispatched, metricsCommandsFailed, metricsEventsDispatched, metricsEventsFailed)
}
