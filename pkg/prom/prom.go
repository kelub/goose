package prom

import (
	"time"

	prom "github.com/prometheus/client_golang/prometheus"
)

// PromVec prometheus metricVec
// This struct represents a collection of Prometheus metrics that can be registered and used in the application.
// It provides methods to register and update different types of metrics such as gauges, counters, and histograms.
type PromVec struct {
	namespace string
	subsystem string

	gauge     *prom.GaugeVec
	counter   *prom.CounterVec
	histogram *prom.HistogramVec
}

// NewPromVec return PromVec with namespace
func NewPromVec(namespace string) *PromVec {
	return &PromVec{
		namespace: namespace,
	}
}

// Namespace set namespace
func (p *PromVec) Namespace(namespace string) *PromVec {
	p.namespace = namespace
	return p
}

// Subsystem set subsystem
func (p *PromVec) Subsystem(subsystem string) *PromVec {
	p.subsystem = subsystem
	return p
}

// Gauge Register GaugeVec with name and labels
// This function registers a GaugeVec with the given name, help string, and labels.
// It returns the PromVec instance for chaining.
func (p *PromVec) Gauge(name string, help string, labels []string) *PromVec {
	if p == nil || p.gauge != nil {
		return p
	}
	p.gauge = prom.NewGaugeVec(
		prom.GaugeOpts{
			Namespace: p.namespace,
			Subsystem: "",
			Name:      name,
			Help:      help,
		}, labels)
	prom.MustRegister(p.gauge)
	return p
}

// Counter Register CounterVec with name and labels
// This function registers a CounterVec with the given name, help string, and labels.
// It returns the PromVec instance for chaining.
func (p *PromVec) Counter(name string, help string, labels []string) *PromVec {
	if p == nil || p.counter != nil {
		return p
	}
	p.counter = prom.NewCounterVec(
		prom.CounterOpts{
			Namespace: p.namespace,
			Subsystem: "",
			Name:      name,
			Help:      help,
		}, labels)
	prom.MustRegister(p.counter)
	return p
}

// Histogram Register HistogramVec with name,labels,buckets
// This function registers a HistogramVec with the given name, help string, labels, and buckets.
// It returns the PromVec instance for chaining.
func (p *PromVec) Histogram(name string, help string, labels []string, buckets []float64) *PromVec {
	if p == nil || p.histogram != nil {
		return p
	}
	p.histogram = prom.NewHistogramVec(
		prom.HistogramOpts{
			Namespace: p.namespace,
			Subsystem: "",
			Name:      name,
			Help:      help,
			Buckets:   buckets,
		}, labels)
	prom.MustRegister(p.histogram)
	return p
}

// Inc inc counter and gauge
// This function increments both the counter and gauge for the given labels.
// It returns the PromVec instance for chaining.
func (p *PromVec) Inc(labels ...string) {
	if p.counter != nil {
		p.counter.WithLabelValues(labels...).Inc()
	}
	if p.gauge != nil {
		p.gauge.WithLabelValues(labels...).Inc()
	}
}

// Dec dec gauge
// This function decrements the gauge for the given labels.
func (p *PromVec) Dec(labels ...string) {
	if p.gauge != nil {
		p.gauge.WithLabelValues(labels...).Dec()
	}
}

// Add both add value to counter and gauge
// value must > 0
// This function adds a value to both the counter and gauge for the given labels.
func (p *PromVec) Add(value float64, labels ...string) {
	if p.counter != nil {
		p.counter.WithLabelValues(labels...).Add(value)
	}
	if p.gauge != nil {
		p.gauge.WithLabelValues(labels...).Add(value)
	}
}

// Set only set value to gauge
// This function sets the value of the gauge for the given labels.
func (p *PromVec) Set(value float64, labels ...string) {
	if p.gauge != nil {
		p.gauge.WithLabelValues(labels...).Set(value)
	}
}

// HandleTime Observe histogram
// The time unit is seconds
// This function observes the histogram for the given labels.
func (p *PromVec) HandleTime(start time.Time, labels ...string) {
	p.histogram.WithLabelValues(labels...).Observe(time.Since(start).Seconds())
}

// HandleTimeWithSeconds Observe histogram
// start must seconds
// This function observes the histogram for the given labels.
func (p *PromVec) HandleTimeWithSeconds(start float64, labels ...string) {
	p.histogram.WithLabelValues(labels...).Observe(start)
}
