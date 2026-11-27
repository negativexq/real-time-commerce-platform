package idempotency

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/negativexq/real-time-commerce-platform/services/event_processor_go/internal/config"
	"github.com/negativexq/real-time-commerce-platform/services/event_processor_go/internal/model"
	"github.com/negativexq/real-time-commerce-platform/services/event_processor_go/internal/procerr"
	"github.com/negativexq/real-time-commerce-platform/services/event_processor_go/internal/testenv"
)

// These tests run the real Lua scripts against Redis (see testenv). They
// port tests/unit/test_processor_idempotency.py, which used a Python fake.

var (
	eventID = uuid.MustParse("00000000-0000-4000-8000-000000000001")
	now     = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	record  = &model.ConsumedMessage{Topic: "commerce.events", Partition: 0, Offset: 1}
	ctx     = context.Background()
)

func store(t *testing.T, env map[string]string) *Store {
	t.Helper()
	values := map[string]string{
		"REDIS_URL":                                    testenv.RedisURL(t),
		"PROCESSOR_IDEMPOTENCY_KEY_PREFIX":             "commerce:processor:gotest:" + testenv.UniqueSuffix(),
		"PROCESSOR_CONSUMER_GROUP":                     "group-a",
		"PROCESSOR_IDEMPOTENCY_PROCESSING_TTL_SECONDS": "10",
		"PROCESSOR_IDEMPOTENCY_COMPLETED_TTL_SECONDS":  "100",
	}
	for k, v := range env {
		values[k] = v
	}
	cfg, err := config.LoadProcessor(nil, func(n string) (string, bool) { v, ok := values[n]; return v, ok })
	if err != nil {
		t.Fatal(err)
	}
	s, err := New(cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Ping(ctx); err != nil {
		t.Fatalf("Redis at PROCESSOR_TEST_REDIS_URL is unreachable: %v", err)
	}
	t.Cleanup(func() {
		_ = s.client.Del(ctx, s.key(eventID)).Err()
		_ = s.Close()
	})
	return s
}

func ttl(t *testing.T, s *Store) time.Duration {
	t.Helper()
	d, err := s.client.TTL(ctx, s.key(eventID)).Result()
	if err != nil {
		t.Fatal(err)
	}
	return d
}

func TestReserveCompleteAndCompletedDuplicate(t *testing.T) {
	s := store(t, nil)
	state, err := s.Reserve(ctx, eventID, "owner", record, now)
	if err != nil || state != Reserved {
		t.Fatalf("reserve: %s %v", state, err)
	}
	if d := ttl(t, s); d <= 0 || d > 10*time.Second {
		t.Fatalf("processing TTL %v", d)
	}
	stored, _ := s.client.Get(ctx, s.key(eventID)).Result()
	want := `{"consumer_group":"group-a","first_seen_at":"2026-01-01T00:00:00+00:00","offset":1,` +
		`"partition":0,"status":"processing","token":"owner","topic":"commerce.events"}`
	if stored != want {
		t.Fatalf("lease value\n got: %s\nwant: %s", stored, want)
	}
	ok, err := s.Complete(ctx, eventID, "owner", now)
	if err != nil || !ok {
		t.Fatalf("complete: %v %v", ok, err)
	}
	if d := ttl(t, s); d <= 10*time.Second || d > 100*time.Second {
		t.Fatalf("completed TTL %v", d)
	}
	stored, _ = s.client.Get(ctx, s.key(eventID)).Result()
	if want := `{"completed_at":"2026-01-01T00:00:00+00:00","consumer_group":"group-a","status":"completed"}`; stored != want {
		t.Fatalf("completed value %s", stored)
	}
	if state, _ := s.Reserve(ctx, eventID, "other", record, now); state != Completed {
		t.Fatalf("duplicate reserve: %s", state)
	}
	if ok, _ := s.Release(ctx, eventID, "owner"); ok {
		t.Fatal("a completed record was released")
	}
}

func TestActiveLeaseAndWrongTokenCannotMutate(t *testing.T) {
	s := store(t, nil)
	if state, _ := s.Reserve(ctx, eventID, "owner", record, now); state != Reserved {
		t.Fatalf("first reserve: %s", state)
	}
	if state, _ := s.Reserve(ctx, eventID, "other", record, now); state != Processing {
		t.Fatalf("second reserve: %s", state)
	}
	if ok, _ := s.Complete(ctx, eventID, "wrong", now); ok {
		t.Fatal("wrong token completed the lease")
	}
	if ok, _ := s.Release(ctx, eventID, "wrong"); ok {
		t.Fatal("wrong token released the lease")
	}
	if ok, _ := s.Release(ctx, eventID, "owner"); !ok {
		t.Fatal("owner could not release")
	}
	if ok, _ := s.Complete(ctx, eventID, "owner", now); ok {
		t.Fatal("released lease was completed")
	}
	if state, _ := s.Reserve(ctx, eventID, "new-owner", record, now); state != Reserved {
		t.Fatalf("reserve after release: %s", state)
	}
}

func TestEventKeyIsNamespacedAndGlobalAcrossGroups(t *testing.T) {
	prefix := "commerce:test:" + testenv.UniqueSuffix()
	first := store(t, map[string]string{"PROCESSOR_IDEMPOTENCY_KEY_PREFIX": prefix, "PROCESSOR_CONSUMER_GROUP": "primary"})
	second := store(t, map[string]string{"PROCESSOR_IDEMPOTENCY_KEY_PREFIX": prefix, "PROCESSOR_CONSUMER_GROUP": "verification"})
	if first.key(eventID) != prefix+":"+eventID.String() || first.key(eventID) != second.key(eventID) {
		t.Fatalf("keys %s / %s", first.key(eventID), second.key(eventID))
	}
	if state, _ := first.Reserve(ctx, eventID, "a", record, now); state != Reserved {
		t.Fatalf("first group: %s", state)
	}
	if state, _ := second.Reserve(ctx, eventID, "b", record, now); state != Processing {
		t.Fatalf("second group saw %s; leases must be shared across groups", state)
	}
}

func TestUnavailableRedisIsRetryable(t *testing.T) {
	cfg, err := config.LoadProcessor(nil, func(n string) (string, bool) {
		if n == "REDIS_URL" {
			return "redis://127.0.0.1:1/0", true
		}
		if n == "PROCESSOR_REDIS_CONNECT_TIMEOUT_SECONDS" {
			return "0.2", true
		}
		return "", false
	})
	if err != nil {
		t.Fatal(err)
	}
	s, err := New(cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	_, err = s.Reserve(ctx, eventID, "owner", record, now)
	if !procerr.IsRetryable(err) {
		t.Fatalf("error %v is not retryable", err)
	}
}
