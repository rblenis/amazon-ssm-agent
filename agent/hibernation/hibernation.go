// Copyright 2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"). You may not
// use this file except in compliance with the License. A copy of the
// License is located at
//
// http://aws.amazon.com/apache2.0/
//
// or in the "license" file accompanying this file. This file is distributed
// on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing
// permissions and limitations under the License.

// Package hibernation is responsible for the agent in hibernate mode.
// While hibernating, the agent sends health pings with a doubling, jittered
// backoff and leaves hibernation as soon as one of them succeeds.
package hibernation

import (
	"math/rand"
	"time"

	"github.com/aws/amazon-ssm-agent/agent/appconfig"
	"github.com/aws/amazon-ssm-agent/agent/context"
	"github.com/aws/amazon-ssm-agent/agent/health"
	"github.com/aws/amazon-ssm-agent/agent/log/logger"
	"github.com/cihub/seelog"
)

// IHibernate holds information about the current agent state
type IHibernate interface {
	ExecuteHibernation(context context.T) health.AgentState
}

const (
	hibernateLogFile = "hibernate.log"
	secondsInMinute  = 60

	// initialRetryInterval is the delay before the first health ping after entering hibernation.
	initialRetryInterval = 15 * time.Second
	// multiplier is applied to the retry interval after every failed health ping.
	multiplier = 2
	// jitterFraction randomizes each wait by +/- this fraction so a fleet does not retry in lockstep.
	jitterFraction = 0.2
)

// Hibernate keeps the agent passive and polls the service until it is reachable again.
type Hibernate struct {
	healthModule health.IHealthCheck

	initialInterval time.Duration
	maxInterval     time.Duration

	// Overridable for tests.
	randFloat func() float64
	newTimer  func(d time.Duration) *time.Timer

	seelogger seelog.LoggerInterface
}

// NewHibernateMode creates an object of type Hibernate
func NewHibernateMode(healthModule health.IHealthCheck, context context.T) *Hibernate {
	context.Log().Debug("Initializing agent hibernate mode. Switching log to minimal logging...")
	maxMinutes := context.AppConfig().Ssm.HibernationMaxBackoffIntervalMinutes
	if maxMinutes <= 0 {
		// The config parser enforces a minimum; guard against an unparsed config so we never spin.
		maxMinutes = appconfig.DefaultHibernationMaxBackoffIntervalMinutes
	}
	maxInterval := time.Duration(maxMinutes*secondsInMinute) * time.Second
	initialInterval := initialRetryInterval
	if maxInterval < initialInterval {
		initialInterval = maxInterval
	}
	return &Hibernate{
		healthModule:    healthModule,
		initialInterval: initialInterval,
		maxInterval:     maxInterval,
		randFloat:       rand.Float64,
		newTimer:        time.NewTimer,
	}
}

// ExecuteHibernation blocks until a health ping succeeds, then returns health.Active so the agent can start.
// Pings are sent after initialInterval, then at doubling intervals capped at maxInterval, each with jitter.
// Every ping result is acted on immediately; there is no fixed blocking window.
func (m *Hibernate) ExecuteHibernation(context context.T) health.AgentState {
	m.seelogger = logger.GetLogger(context.Log(), getHibernateSeelogConfig())
	defer m.seelogger.Close()
	m.seelogger.Infof("Agent is in hibernate mode. Retrying health check after %v, doubling up to every %v.",
		m.initialInterval, m.maxInterval)

	interval := m.initialInterval
	for attempt := 1; ; attempt++ {
		timer := m.newTimer(m.jitter(interval))
		<-timer.C

		status, err := m.healthModule.GetAgentState()
		if status == health.Active {
			m.seelogger.Infof("Health check succeeded on attempt %d. Leaving hibernate mode.", attempt)
			return health.Active
		}

		interval = m.nextInterval(interval)
		if err != nil {
			// Before the cap this logs a handful of lines; at the cap, one line per max interval.
			m.seelogger.Errorf("Health ping failed (attempt %d), next retry in about %v - %v", attempt, interval, err)
		}
	}
}

// nextInterval doubles the interval, capped at maxInterval.
func (m *Hibernate) nextInterval(interval time.Duration) time.Duration {
	next := interval * multiplier
	if next > m.maxInterval || next <= 0 {
		next = m.maxInterval
	}
	return next
}

// jitter returns interval randomized by +/- jitterFraction, never above maxInterval.
func (m *Hibernate) jitter(interval time.Duration) time.Duration {
	delta := (m.randFloat()*2 - 1) * jitterFraction // in [-jitterFraction, +jitterFraction)
	wait := time.Duration(float64(interval) * (1 + delta))
	if wait > m.maxInterval {
		wait = m.maxInterval
	}
	return wait
}
