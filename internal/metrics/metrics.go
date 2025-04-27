package metrics

import (
	"fmt"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Metrics handles broker metrics
type Metrics struct {
	// Message metrics
	MessagesPublished prometheus.Counter
	MessagesConsumed  prometheus.Counter
	MessagesFailed    prometheus.Counter
	MessageLatency    prometheus.Histogram
	MessageSize       prometheus.Histogram

	// Topic metrics
	TopicsCreated      prometheus.Counter
	TopicsDeleted      prometheus.Counter
	PartitionsPerTopic prometheus.Gauge

	// Consumer metrics
	ConsumerGroups    prometheus.Gauge
	ConsumersPerGroup prometheus.Gauge
	ConsumerLag       prometheus.Gauge

	// System metrics
	MemoryUsage       prometheus.Gauge
	CPUUsage          prometheus.Gauge
	DiskUsage         prometheus.Gauge
	ActiveConnections prometheus.Gauge

	// Custom metrics
	mu            sync.RWMutex
	customMetrics map[string]prometheus.Collector
}

// NewMetrics creates a new metrics instance
func NewMetrics() *Metrics {
	return &Metrics{
		MessagesPublished: promauto.NewCounter(prometheus.CounterOpts{
			Name: "voltaxmq_messages_published_total",
			Help: "Total number of messages published",
		}),
		MessagesConsumed: promauto.NewCounter(prometheus.CounterOpts{
			Name: "voltaxmq_messages_consumed_total",
			Help: "Total number of messages consumed",
		}),
		MessagesFailed: promauto.NewCounter(prometheus.CounterOpts{
			Name: "voltaxmq_messages_failed_total",
			Help: "Total number of messages that failed processing",
		}),
		MessageLatency: promauto.NewHistogram(prometheus.HistogramOpts{
			Name:    "voltaxmq_message_latency_seconds",
			Help:    "Message processing latency in seconds",
			Buckets: prometheus.DefBuckets,
		}),
		MessageSize: promauto.NewHistogram(prometheus.HistogramOpts{
			Name:    "voltaxmq_message_size_bytes",
			Help:    "Message size in bytes",
			Buckets: prometheus.ExponentialBuckets(100, 10, 8),
		}),
		TopicsCreated: promauto.NewCounter(prometheus.CounterOpts{
			Name: "voltaxmq_topics_created_total",
			Help: "Total number of topics created",
		}),
		TopicsDeleted: promauto.NewCounter(prometheus.CounterOpts{
			Name: "voltaxmq_topics_deleted_total",
			Help: "Total number of topics deleted",
		}),
		PartitionsPerTopic: promauto.NewGauge(prometheus.GaugeOpts{
			Name: "voltaxmq_partitions_per_topic",
			Help: "Number of partitions per topic",
		}),
		ConsumerGroups: promauto.NewGauge(prometheus.GaugeOpts{
			Name: "voltaxmq_consumer_groups",
			Help: "Number of consumer groups",
		}),
		ConsumersPerGroup: promauto.NewGauge(prometheus.GaugeOpts{
			Name: "voltaxmq_consumers_per_group",
			Help: "Number of consumers per group",
		}),
		ConsumerLag: promauto.NewGauge(prometheus.GaugeOpts{
			Name: "voltaxmq_consumer_lag",
			Help: "Consumer lag in messages",
		}),
		MemoryUsage: promauto.NewGauge(prometheus.GaugeOpts{
			Name: "voltaxmq_memory_usage_bytes",
			Help: "Memory usage in bytes",
		}),
		CPUUsage: promauto.NewGauge(prometheus.GaugeOpts{
			Name: "voltaxmq_cpu_usage_percent",
			Help: "CPU usage percentage",
		}),
		DiskUsage: promauto.NewGauge(prometheus.GaugeOpts{
			Name: "voltaxmq_disk_usage_bytes",
			Help: "Disk usage in bytes",
		}),
		ActiveConnections: promauto.NewGauge(prometheus.GaugeOpts{
			Name: "voltaxmq_active_connections",
			Help: "Number of active connections",
		}),
		customMetrics: make(map[string]prometheus.Collector),
	}
}

// RecordMessagePublished records a published message
func (m *Metrics) RecordMessagePublished(size int) {
	m.MessagesPublished.Inc()
	m.MessageSize.Observe(float64(size))
}

// RecordMessageConsumed records a consumed message
func (m *Metrics) RecordMessageConsumed(latency time.Duration) {
	m.MessagesConsumed.Inc()
	m.MessageLatency.Observe(latency.Seconds())
}

// RecordMessageFailed records a failed message
func (m *Metrics) RecordMessageFailed() {
	m.MessagesFailed.Inc()
}

// RecordTopicCreated records a created topic
func (m *Metrics) RecordTopicCreated(partitions int) {
	m.TopicsCreated.Inc()
	m.PartitionsPerTopic.Set(float64(partitions))
}

// RecordTopicDeleted records a deleted topic
func (m *Metrics) RecordTopicDeleted() {
	m.TopicsDeleted.Inc()
}

// UpdateConsumerMetrics updates consumer-related metrics
func (m *Metrics) UpdateConsumerMetrics(groups, consumersPerGroup, lag int) {
	m.ConsumerGroups.Set(float64(groups))
	m.ConsumersPerGroup.Set(float64(consumersPerGroup))
	m.ConsumerLag.Set(float64(lag))
}

// UpdateSystemMetrics updates system-related metrics
func (m *Metrics) UpdateSystemMetrics(memory, cpu, disk, connections int) {
	m.MemoryUsage.Set(float64(memory))
	m.CPUUsage.Set(float64(cpu))
	m.DiskUsage.Set(float64(disk))
	m.ActiveConnections.Set(float64(connections))
}

// AddCustomMetric adds a custom metric
func (m *Metrics) AddCustomMetric(name string, metric prometheus.Collector) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.customMetrics[name]; exists {
		return fmt.Errorf("metric %s already exists", name)
	}

	m.customMetrics[name] = metric
	return nil
}

// GetCustomMetric gets a custom metric
func (m *Metrics) GetCustomMetric(name string) (prometheus.Collector, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	metric, exists := m.customMetrics[name]
	if !exists {
		return nil, fmt.Errorf("metric %s not found", name)
	}

	return metric, nil
}
