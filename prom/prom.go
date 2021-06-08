package prom

import (
	"time"

	prom "github.com/prometheus/client_golang/prometheus"
)

// PromVec prometheus metricVec
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
func (p *PromVec) Inc(labels ...string) {
	if p.counter != nil {
		p.counter.WithLabelValues(labels...).Inc()
	}
	if p.gauge != nil {
		p.gauge.WithLabelValues(labels...).Inc()
	}
}

// Dec dec gauge
func (p *PromVec) Dec(labels ...string) {
	if p.gauge != nil {
		p.gauge.WithLabelValues(labels...).Dec()
	}
}

// Add both add value to counter and gauge
// value must > 0
func (p *PromVec) Add(value float64, labels ...string) {
	if p.counter != nil {
		p.counter.WithLabelValues(labels...).Add(value)
	}
	if p.gauge != nil {
		p.gauge.WithLabelValues(labels...).Add(value)
	}
}

// Set only set value to gauge
func (p *PromVec) Set(value float64, labels ...string) {
	if p.gauge != nil {
		p.gauge.WithLabelValues(labels...).Set(value)
	}
}

// HandleTime Observe histogram
// The time unit is seconds
func (p *PromVec) HandleTime(start time.Time, labels ...string) {
	p.histogram.WithLabelValues(labels...).Observe(time.Since(start).Seconds())
}

// HandleTimeWithSeconds Observe histogram
// start must seconds
func (p *PromVec) HandleTimeWithSeconds(start float64, labels ...string) {
	p.histogram.WithLabelValues(labels...).Observe(start)
}
