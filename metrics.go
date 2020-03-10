package main

import (
	"net/http"
	"strconv"
	"time"

	"rrinterceptor/promutils"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/prometheus/prometheus/prompb"
)

var (
	promRegistry *prometheus.Registry
	driftMetric  *prometheus.CounterVec
)

func initMetrics() (err error) {
	driftMetric = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "rrinterceptor",
		Subsystem: "queries",
		Name:      "startdrift",
		Help:      "Returns the number of requests splitted by start drift and stepping. Drift is the number of hours between query start and now rounded to 3 hours. Stepping is the number of seconds used as stepping, rouded to 15s (0 if not present).",
		// ConstLabels: nil,
	}, []string{
		"drift",
		"step",
	})
	promRegistry = prometheus.NewRegistry()
	return promRegistry.Register(driftMetric)
}

func promHandler() http.Handler {
	return promhttp.HandlerFor(promRegistry, promhttp.HandlerOpts{})
}

func updateDriftStats(req prompb.ReadRequest) {
	var (
		drift, step            time.Duration
		hourDrift, secondsStep int64
	)
	now := time.Now()
	for _, query := range req.Queries {
		// Let's be safe
		if query == nil {
			continue
		}
		// Compute rounded drift and stepping to get meaningfull values
		drift = now.Sub(promutils.GetTimeFromTS(promutils.GetEffectiveStart(query)))
		hourDrift = int64(drift.Round(3*time.Hour) / time.Hour)
		if promutils.IsSteppingUsable(query) {
			step = time.Duration(query.Hints.StepMs) * time.Millisecond
			secondsStep = int64(step.Round(15*time.Second) / time.Second)
		} else {
			secondsStep = 0
		}
		// Update stats
		driftMetric.WithLabelValues(strconv.FormatInt(hourDrift, 10), strconv.FormatInt(secondsStep, 10)).Inc()
		log.Debugf("[Metrics] Incrementing the counter for startDrift metric with dimension: drift(%dh) step(%ds)",
			hourDrift, secondsStep)
	}
}
