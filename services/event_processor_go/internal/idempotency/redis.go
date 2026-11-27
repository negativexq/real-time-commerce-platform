// Package idempotency ports services.event_processor.idempotency: token-
// guarded Redis leases using the identical Lua scripts and key/value shape,
// so Python and Go processors can share one Redis keyspace.
package idempotency

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"

	"github.com/negativexq/real-time-commerce-platform/services/event_processor_go/internal/config"
	"github.com/negativexq/real-time-commerce-platform/services/event_processor_go/internal/contracts"
	"github.com/negativexq/real-time-commerce-platform/services/event_processor_go/internal/metrics"
	"github.com/negativexq/real-time-commerce-platform/services/event_processor_go/internal/model"
	"github.com/negativexq/real-time-commerce-platform/services/event_processor_go/internal/procerr"
)

var reserveScript = redis.NewScript(`
local current = redis.call('GET', KEYS[1])
if not current then
  redis.call('SET', KEYS[1], ARGV[1], 'EX', ARGV[2], 'NX')
  return {'reserved', ARGV[1]}
end
local parsed = cjson.decode(current)
if parsed.status == 'completed' then return {'completed', current} end
return {'processing', current}
`)

var completeScript = redis.NewScript(`
local current = redis.call('GET', KEYS[1])
if not current then return 0 end
local parsed = cjson.decode(current)
if parsed.status ~= 'processing' or parsed.token ~= ARGV[1] then return 0 end
redis.call('SET', KEYS[1], ARGV[2], 'EX', ARGV[3])
return 1
`)

var releaseScript = redis.NewScript(`
local current = redis.call('GET', KEYS[1])
if not current then return 0 end
local parsed = cjson.decode(current)
if parsed.status ~= 'processing' or parsed.token ~= ARGV[1] then return 0 end
return redis.call('DEL', KEYS[1])
`)

type ReservationState string

const (
	Reserved   ReservationState = "reserved"
	Completed  ReservationState = "completed"
	Processing ReservationState = "processing"
)

type Store struct {
	cfg     *config.Processor
	client  *redis.Client
	metrics *metrics.Metrics
}

func New(cfg *config.Processor, m *metrics.Metrics) (*Store, error) {
	opts, err := redis.ParseURL(cfg.RedisURL)
	if err != nil {
		return nil, fmt.Errorf("invalid REDIS_URL: %w", err)
	}
	opts.DialTimeout = cfg.RedisConnectTimeout
	opts.ReadTimeout = cfg.RedisSocketTimeout
	opts.WriteTimeout = cfg.RedisSocketTimeout
	opts.PoolSize = 2
	opts.MaxRetries = -1 // never silently re-run a script; the processor owns retries
	return &Store{cfg: cfg, client: redis.NewClient(opts), metrics: m}, nil
}

func (s *Store) key(eventID uuid.UUID) string {
	return s.cfg.IdempotencyKeyPrefix + ":" + eventID.String()
}

func (s *Store) Ping(ctx context.Context) error {
	if err := s.client.Ping(ctx).Err(); err != nil {
		return procerr.Wrap(procerr.RetryableProcessing, "Redis is unavailable", err)
	}
	return nil
}

func (s *Store) Reserve(ctx context.Context, eventID uuid.UUID, token string, m *model.ConsumedMessage, firstSeen time.Time) (ReservationState, error) {
	started := time.Now()
	value := contracts.CanonicalJSON(contracts.Object{
		"status":         "processing",
		"token":          token,
		"consumer_group": s.cfg.ConsumerGroup,
		"topic":          m.Topic,
		"partition":      int64(m.Partition),
		"offset":         m.Offset,
		"first_seen_at":  contracts.FormatISOOffset(firstSeen),
	})
	raw, err := reserveScript.Run(ctx, s.client, []string{s.key(eventID)}, string(value), s.cfg.IdempotencyProcessingTTL).Slice()
	if err != nil {
		s.observe("reserve", "unavailable", started)
		return "", procerr.Wrap(procerr.RetryableProcessing, "Redis reservation failed", err)
	}
	if len(raw) != 2 {
		return "", procerr.New(procerr.RetryableProcessing, "Redis reservation returned invalid data")
	}
	state, _ := raw[0].(string)
	result := map[string]string{"reserved": "reserved", "completed": "duplicate_completed", "processing": "duplicate_processing"}[state]
	if result == "" {
		return "", procerr.New(procerr.RetryableProcessing, "Redis reservation returned invalid data")
	}
	s.observe("reserve", result, started)
	return ReservationState(state), nil
}

func (s *Store) Complete(ctx context.Context, eventID uuid.UUID, token string, completedAt time.Time) (bool, error) {
	started := time.Now()
	value := contracts.CanonicalJSON(contracts.Object{
		"status":         "completed",
		"consumer_group": s.cfg.ConsumerGroup,
		"completed_at":   contracts.FormatISOOffset(completedAt),
	})
	n, err := completeScript.Run(ctx, s.client, []string{s.key(eventID)}, token, string(value), s.cfg.IdempotencyCompletedTTL).Int64()
	if err != nil {
		s.observe("complete", "unavailable", started)
		return false, procerr.Wrap(procerr.RetryableProcessing, "Redis completion failed", err)
	}
	result := "token_mismatch"
	if n != 0 {
		result = "completed"
	}
	s.observe("complete", result, started)
	return n != 0, nil
}

func (s *Store) Release(ctx context.Context, eventID uuid.UUID, token string) (bool, error) {
	started := time.Now()
	n, err := releaseScript.Run(ctx, s.client, []string{s.key(eventID)}, token).Int64()
	if err != nil {
		s.observe("reconcile", "unavailable", started)
		return false, procerr.Wrap(procerr.RetryableProcessing, "Redis release failed", err)
	}
	result := "token_mismatch"
	if n != 0 {
		result = "released"
	}
	s.observe("reconcile", result, started)
	return n != 0, nil
}

func (s *Store) observe(operation, result string, started time.Time) {
	if s.metrics == nil {
		return
	}
	d := time.Since(started).Seconds()
	s.metrics.IdempotencyOps.WithLabelValues(operation, result).Inc()
	s.metrics.IdempotencyDuration.WithLabelValues(operation, result).Observe(d)
	s.metrics.RedisDuration.WithLabelValues(operation, result).Observe(d)
}

func (s *Store) Close() error { return s.client.Close() }
