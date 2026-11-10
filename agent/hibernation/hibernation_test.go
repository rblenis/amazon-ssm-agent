// // Copyright 2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
// //
// // Licensed under the Apache License, Version 2.0 (the "License"). You may not
// // use this file except in compliance with the License. A copy of the
// // License is located at
// //
// // http://aws.amazon.com/apache2.0/
// //
// // or in the "license" file accompanying this file. This file is distributed
// // on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// // either express or implied. See the License for the specific language governing
// // permissions and limitations under the License.

package hibernation

import (
	"errors"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/aws/amazon-ssm-agent/agent/appconfig"
	"github.com/aws/amazon-ssm-agent/agent/health"
	healthmocks "github.com/aws/amazon-ssm-agent/agent/health/mocks"
	"github.com/aws/amazon-ssm-agent/agent/mocks/context"
	"github.com/stretchr/testify/assert"
)

var errPing = errors.New("dial tcp: lookup ssm endpoint: connection refused")

// recordingTimers fires every timer immediately and records the requested durations.
type recordingTimers struct {
	sync.Mutex
	waits []time.Duration
}

func (r *recordingTimers) newTimer(d time.Duration) *time.Timer {
	r.Lock()
	r.waits = append(r.waits, d)
	r.Unlock()
	return time.NewTimer(0)
}

// newTestHibernate returns a Hibernate whose health check fails `failures` times and then succeeds.
func newTestHibernate(t *testing.T, failures int, maxInterval time.Duration) (*Hibernate, *healthmocks.IHealthCheck, *recordingTimers) {
	ctx := context.NewMockDefault()
	hm := &healthmocks.IHealthCheck{}
	if failures > 0 {
		hm.On("GetAgentState").Return(health.Passive, errPing).Times(failures)
	}
	hm.On("GetAgentState").Return(health.Active, nil).Once()

	h := NewHibernateMode(hm, ctx)
	h.maxInterval = maxInterval
	timers := &recordingTimers{}
	h.newTimer = timers.newTimer
	h.randFloat = func() float64 { return 0.5 } // no jitter
	return h, hm, timers
}

func TestNewHibernateMode_UsesConfig(t *testing.T) {
	ctx := context.NewMockDefault()
	h := NewHibernateMode(&healthmocks.IHealthCheck{}, ctx)
	assert.Equal(t, initialRetryInterval, h.initialInterval)
	// The mock config leaves the value unset (0), which must fall back to the default rather than 0.
	assert.Equal(t, time.Duration(appconfig.DefaultHibernationMaxBackoffIntervalMinutes)*time.Minute, h.maxInterval)

	cfg := ctx.AppConfig()
	cfg.Ssm.HibernationMaxBackoffIntervalMinutes = 5
	ctx2 := context.NewMockDefaultWithConfig(cfg)
	h2 := NewHibernateMode(&healthmocks.IHealthCheck{}, ctx2)
	assert.Equal(t, 5*time.Minute, h2.maxInterval)
}

func TestHibernation_FirstSuccessEndsHibernationImmediately(t *testing.T) {
	h, hm, timers := newTestHibernate(t, 0, 60*time.Minute)

	status := h.ExecuteHibernation(context.NewMockDefault())

	assert.Equal(t, health.Active, status)
	hm.AssertNumberOfCalls(t, "GetAgentState", 1)
	assert.Equal(t, []time.Duration{initialRetryInterval}, timers.waits)
}

func TestHibernation_IntervalsDoubleAndCapAtMax(t *testing.T) {
	h, hm, timers := newTestHibernate(t, 9, 5*time.Minute)

	status := h.ExecuteHibernation(context.NewMockDefault())

	assert.Equal(t, health.Active, status)
	hm.AssertNumberOfCalls(t, "GetAgentState", 10)
	s := time.Second
	assert.Equal(t, []time.Duration{
		15 * s, 30 * s, 60 * s, 120 * s, 240 * s, // doubling
		300 * s, 300 * s, 300 * s, 300 * s, 300 * s, // capped at max (5 min)
	}, timers.waits)
}

func TestHibernation_DefaultScheduleReachesCap(t *testing.T) {
	h := &Hibernate{maxInterval: time.Duration(appconfig.DefaultHibernationMaxBackoffIntervalMinutes) * time.Minute}
	interval := initialRetryInterval
	var got []time.Duration
	for i := 0; i < 10; i++ {
		got = append(got, interval)
		interval = h.nextInterval(interval)
	}
	assert.Equal(t, h.maxInterval, got[len(got)-1])
	for i := 1; i < len(got); i++ {
		assert.True(t, got[i] == 2*got[i-1] || got[i] == h.maxInterval, "interval %d: %v after %v", i, got[i], got[i-1])
	}
}

func TestHibernation_InitialIntervalNeverExceedsMax(t *testing.T) {
	h := &Hibernate{maxInterval: 10 * time.Second}
	assert.Equal(t, 10*time.Second, h.nextInterval(8*time.Second))
	assert.Equal(t, 10*time.Second, h.nextInterval(10*time.Second))
}

func TestHibernation_JitterStaysWithinBounds(t *testing.T) {
	h := &Hibernate{maxInterval: 60 * time.Minute}
	interval := 4 * time.Minute
	low := time.Duration(float64(interval) * (1 - jitterFraction))
	high := time.Duration(float64(interval) * (1 + jitterFraction))

	h.randFloat = func() float64 { return 0 }
	assert.Equal(t, low, h.jitter(interval))
	h.randFloat = func() float64 { return 0.5 }
	assert.Equal(t, interval, h.jitter(interval))

	r := rand.New(rand.NewSource(1))
	h.randFloat = r.Float64
	seenBelow, seenAbove := false, false
	for i := 0; i < 1000; i++ {
		w := h.jitter(interval)
		assert.GreaterOrEqual(t, w, low)
		assert.Less(t, w, high)
		seenBelow = seenBelow || w < interval
		seenAbove = seenAbove || w > interval
	}
	assert.True(t, seenBelow && seenAbove, "jitter should spread waits on both sides of the interval")
}

func TestHibernation_JitterNeverExceedsMax(t *testing.T) {
	h := &Hibernate{maxInterval: 5 * time.Minute}
	h.randFloat = func() float64 { return 0.999999 }
	assert.Equal(t, 5*time.Minute, h.jitter(5*time.Minute))
}

// Regression test for the old fixed 15-minute blind spot: the agent entered hibernation after a
// transient startup failure (local DNS not yet listening), the service was reachable seconds later,
// but the first backoff window blocked for initialPingRate*backoffRate (15 min) before any ping
// result was read, so the agent only started 20 minutes after hibernating. With real timers and a
// short initial interval, a ping that succeeds on the second attempt must end hibernation promptly.
func TestHibernation_RegressionNoBlindSpotAfterTransientFailure(t *testing.T) {
	ctx := context.NewMockDefault()
	hm := &healthmocks.IHealthCheck{}
	hm.On("GetAgentState").Return(health.Passive, errPing).Once()
	hm.On("GetAgentState").Return(health.Active, nil).Once()

	h := NewHibernateMode(hm, ctx)
	h.initialInterval = 10 * time.Millisecond

	result := make(chan health.AgentState, 1)
	start := time.Now()
	go func() { result <- h.ExecuteHibernation(ctx) }()

	select {
	case status := <-result:
		assert.Equal(t, health.Active, status)
		assert.Less(t, time.Since(start), 2*time.Second)
		hm.AssertNumberOfCalls(t, "GetAgentState", 2)
	case <-time.After(5 * time.Second):
		t.Fatal("hibernation did not end after a successful health ping")
	}
}

// With production defaults, a service that becomes reachable ~16s after hibernation starts
// (the observed boot race) is detected on the second ping, about 45s in, instead of after 20 minutes.
func TestHibernation_DefaultsRecoverQuicklyFromBootRace(t *testing.T) {
	h, _, timers := newTestHibernate(t, 1, time.Duration(appconfig.DefaultHibernationMaxBackoffIntervalMinutes)*time.Minute)

	h.ExecuteHibernation(context.NewMockDefault())

	var total time.Duration
	for _, w := range timers.waits {
		total += w
	}
	assert.Equal(t, 45*time.Second, total)
}
