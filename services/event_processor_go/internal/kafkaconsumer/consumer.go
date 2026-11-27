// Package kafkaconsumer ports services.event_processor.consumer on the same
// librdkafka client the Python service uses, so a benchmark comparison
// isolates the language runtime rather than the Kafka client.
package kafkaconsumer

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"

	"github.com/negativexq/real-time-commerce-platform/services/event_processor_go/internal/config"
	"github.com/negativexq/real-time-commerce-platform/services/event_processor_go/internal/metrics"
	"github.com/negativexq/real-time-commerce-platform/services/event_processor_go/internal/model"
	"github.com/negativexq/real-time-commerce-platform/services/event_processor_go/internal/offsets"
	"github.com/negativexq/real-time-commerce-platform/services/event_processor_go/internal/procerr"
)

type Consumer struct {
	cfg     *config.Processor
	client  *kafka.Consumer
	metrics *metrics.Metrics
	tracker *offsets.Tracker
	revoked map[offsets.PartitionKey]bool
}

func New(cfg *config.Processor, m *metrics.Metrics) (*Consumer, error) {
	client, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":             cfg.KafkaBootstrapServers,
		"group.id":                      cfg.ConsumerGroup,
		"client.id":                     cfg.ClientID,
		"enable.auto.commit":            false,
		"enable.auto.offset.store":      false,
		"auto.offset.reset":             cfg.AutoOffsetReset,
		"isolation.level":               "read_committed",
		"session.timeout.ms":            cfg.SessionTimeoutMs,
		"heartbeat.interval.ms":         cfg.HeartbeatIntervalMs,
		"max.poll.interval.ms":          cfg.MaxPollIntervalMs,
		"partition.assignment.strategy": "cooperative-sticky",
		"statistics.interval.ms":        1000,
	})
	if err != nil {
		return nil, err
	}
	c := &Consumer{cfg: cfg, client: client, metrics: m, revoked: map[offsets.PartitionKey]bool{}}
	c.tracker = offsets.NewTracker(cfg.OffsetCommitBatchSize,
		time.Duration(cfg.OffsetCommitIntervalMs)*time.Millisecond, c.commitOffsets, m, nil)
	return c, nil
}

func (c *Consumer) Subscribe() error {
	return c.client.SubscribeTopics([]string{c.cfg.InputTopic}, c.rebalance)
}

func (c *Consumer) HasAssignment() bool {
	parts, err := c.client.Assignment()
	return err == nil && len(parts) > 0
}

func partitionList(parts []kafka.TopicPartition) []map[string]any {
	out := make([]map[string]any, 0, len(parts))
	for _, p := range parts {
		out = append(out, map[string]any{"topic": *p.Topic, "partition": p.Partition})
	}
	return out
}

// rebalance runs inside Poll(); librdkafka performs the incremental
// (un)assignment after it returns, like the Python on_assign/on_revoke.
func (c *Consumer) rebalance(_ *kafka.Consumer, ev kafka.Event) error {
	switch e := ev.(type) {
	case kafka.AssignedPartitions:
		for _, p := range e.Partitions {
			delete(c.revoked, offsets.PartitionKey{Topic: *p.Topic, Partition: p.Partition})
		}
		slog.Info("partitions_assigned", "partitions", partitionList(e.Partitions))
		if c.metrics != nil {
			c.metrics.Rebalances.WithLabelValues("assigned").Inc()
			c.metrics.Assigned.Set(float64(len(e.Partitions)))
		}
	case kafka.RevokedPartitions:
		keys := make([]offsets.PartitionKey, 0, len(e.Partitions))
		for _, p := range e.Partitions {
			keys = append(keys, offsets.PartitionKey{Topic: *p.Topic, Partition: p.Partition})
		}
		// Flush safe offsets for exactly the revoked partitions before
		// ownership moves; a failure is logged, never retried later.
		if err := c.tracker.FlushPartitions(keys, "rebalance"); err != nil {
			slog.Warn("offset_flush_on_revoke_failed", "partitions", partitionList(e.Partitions))
		}
		for _, k := range keys {
			c.tracker.DropPartition(k)
			c.revoked[k] = true
		}
		slog.Info("partitions_revoked", "partitions", partitionList(e.Partitions))
		if c.metrics != nil {
			c.metrics.Rebalances.WithLabelValues("revoked").Inc()
			c.metrics.Assigned.Set(0)
		}
	}
	return nil
}

// Poll returns at most one record within the poll timeout. Statistics and
// non-fatal client errors are serviced inside the call, matching the Python
// client where they reach callbacks rather than poll()'s return value.
func (c *Consumer) Poll() (*model.ConsumedMessage, error) {
	started := time.Now()
	deadline := started.Add(c.cfg.PollTimeout)
	defer func() {
		if c.metrics != nil {
			c.metrics.LastPoll.SetToCurrentTime()
			c.metrics.PollDuration.Observe(time.Since(started).Seconds())
		}
	}()
	for {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			break
		}
		ev := c.client.Poll(int(remaining.Milliseconds()) + 1)
		switch e := ev.(type) {
		case nil:
			continue
		case *kafka.Message:
			if e.TopicPartition.Error != nil {
				return nil, procerr.Wrap(procerr.FatalInfrastructure, fmt.Sprintf("Kafka consume failed: %v", e.TopicPartition.Error), e.TopicPartition.Error)
			}
			return toConsumed(e), nil
		case kafka.Error:
			if e.Code() == kafka.ErrPartitionEOF {
				continue
			}
			if e.IsFatal() || e.Code() == kafka.ErrUnknownTopicOrPart || e.Code() == kafka.ErrUnknownTopic {
				return nil, procerr.Wrap(procerr.FatalInfrastructure, "Kafka consume failed: "+e.Error(), e)
			}
			slog.Warn("kafka_client_error", "error", e.Error())
		case *kafka.Stats:
			c.recordStats(e.String())
		}
	}
	if c.metrics != nil {
		c.metrics.EmptyPolls.Inc()
	}
	return nil, nil
}

// PollReady returns a record only if one is already fetched, without
// waiting: it fills a batch from the local queue after Poll returned one.
func (c *Consumer) PollReady() (*model.ConsumedMessage, error) {
	for {
		switch e := c.client.Poll(0).(type) {
		case nil:
			return nil, nil
		case *kafka.Message:
			if e.TopicPartition.Error != nil {
				return nil, procerr.Wrap(procerr.FatalInfrastructure, fmt.Sprintf("Kafka consume failed: %v", e.TopicPartition.Error), e.TopicPartition.Error)
			}
			return toConsumed(e), nil
		case kafka.Error:
			if e.Code() == kafka.ErrPartitionEOF {
				return nil, nil
			}
			if e.IsFatal() || e.Code() == kafka.ErrUnknownTopicOrPart || e.Code() == kafka.ErrUnknownTopic {
				return nil, procerr.Wrap(procerr.FatalInfrastructure, "Kafka consume failed: "+e.Error(), e)
			}
			slog.Warn("kafka_client_error", "error", e.Error())
		case *kafka.Stats:
			c.recordStats(e.String())
		}
	}
}

func toConsumed(m *kafka.Message) *model.ConsumedMessage {
	headers := make([]model.Header, len(m.Headers))
	for i, h := range m.Headers {
		headers[i] = model.Header{Key: h.Key, Value: h.Value}
	}
	var ts *time.Time
	if m.TimestampType != kafka.TimestampNotAvailable && !m.Timestamp.IsZero() && m.Timestamp.UnixMilli() >= 0 {
		t := m.Timestamp.UTC()
		ts = &t
	}
	return &model.ConsumedMessage{
		Topic: *m.TopicPartition.Topic, Partition: m.TopicPartition.Partition,
		Offset: int64(m.TopicPartition.Offset), Timestamp: ts,
		Key: m.Key, Value: m.Value, Headers: headers,
	}
}

type statsPayload struct {
	Topics map[string]struct {
		Partitions map[string]struct {
			FetchqCnt *int64 `json:"fetchq_cnt"`
		} `json:"partitions"`
	} `json:"topics"`
	Brokers map[string]struct {
		RTT *struct {
			Avg float64 `json:"avg"`
			Cnt int64   `json:"cnt"`
		} `json:"rtt"`
	} `json:"brokers"`
}

func (c *Consumer) recordStats(raw string) {
	if c.metrics == nil {
		return
	}
	var s statsPayload
	if json.Unmarshal([]byte(raw), &s) != nil {
		return
	}
	var total int64
	for _, t := range s.Topics {
		for _, p := range t.Partitions {
			if p.FetchqCnt != nil {
				total += *p.FetchqCnt
			}
		}
	}
	c.metrics.FetchqRecords.Set(float64(total))
	sum, n := 0.0, 0
	for _, b := range s.Brokers {
		if b.RTT != nil && b.RTT.Cnt > 0 {
			sum += b.RTT.Avg / 1000
			n++
		}
	}
	if n > 0 {
		c.metrics.BrokerRTTMs.Set(sum / float64(n))
	}
}

// CommitTerminal marks a terminal record safe and flushes on threshold.
func (c *Consumer) CommitTerminal(m *model.ConsumedMessage) error {
	if c.revoked[offsets.PartitionKey{Topic: m.Topic, Partition: m.Partition}] {
		return procerr.New(procerr.FatalInfrastructure, "partition was revoked before commit")
	}
	c.tracker.MarkTerminal(m.Topic, m.Partition, m.Offset)
	return c.tracker.MaybeFlush()
}

func (c *Consumer) commitOffsets(commits map[offsets.PartitionKey]int64) error {
	parts := make([]kafka.TopicPartition, 0, len(commits))
	for key, next := range commits {
		topic := key.Topic
		parts = append(parts, kafka.TopicPartition{Topic: &topic, Partition: key.Partition, Offset: kafka.Offset(next)})
	}
	_, err := c.client.CommitOffsets(parts)
	return err
}

// MaybeFlushIdle keeps the interval guarantee on wall-clock time when idle.
func (c *Consumer) MaybeFlushIdle() error { return c.tracker.MaybeFlush() }

func (c *Consumer) FlushPending(reason string) error { return c.tracker.FlushAll(reason) }

func (c *Consumer) Close() error { return c.client.Close() }
