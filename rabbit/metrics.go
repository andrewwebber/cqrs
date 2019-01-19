package rabbit

import "github.com/prometheus/client_golang/prometheus"

var (
	metricsCommandsPublished *prometheus.CounterVec
	metricsCommandsFailed    *prometheus.CounterVec
	metricsEventsPublished   *prometheus.CounterVec
	metricsEventsFailed      *prometheus.CounterVec
)

func init() {
	metricsCommandsPublished = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name:      "rabbit_commands_published",
		Subsystem: "ix",
		Help:      "CQRS Commands Published",
	}, []string{"command"})

	metricsCommandsFailed = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name:      "rabbit_commands_failed",
		Subsystem: "ix",
		Help:      "CQRS Commands Failed",
	}, []string{"command"})

	metricsEventsPublished = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name:      "rabbit_events_published",
		Subsystem: "ix",
		Help:      "CQRS Events Published",
	}, []string{"event"})

	metricsEventsFailed = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name:      "rabbit_events_failed",
		Subsystem: "ix",
		Help:      "CQRS Events Failed",
	}, []string{"event"})

	prometheus.MustRegister(metricsCommandsPublished, metricsCommandsFailed, metricsEventsPublished, metricsEventsFailed)
}
