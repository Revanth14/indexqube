package telemetry

import "github.com/prometheus/client_golang/prometheus"

// Metrics is the registry of pre-declared collectors the gateway emits to.
// Adding a new metric: declare a field, register it in newMetrics, and
// expose via dotted-name from the call site.
//
// Bucket choices: HTTPDuration spans 1ms..300s in roughly 2.5x steps so
// both sub-15ms proxy paths and long inference streams have signal.
type Metrics struct {
	// HTTPRequestsTotal counts every request reaching the public mux.
	// Labels: method, route (the registered ServeMux pattern, NOT raw URL),
	// status (HTTP status code as string).
	HTTPRequestsTotal *prometheus.CounterVec

	// HTTPDuration captures wall-clock latency from request handler entry
	// to handler return (which for streams is the end of the stream).
	HTTPDuration *prometheus.HistogramVec

	// StreamsActive is the count of in-flight streaming requests right now.
	StreamsActive prometheus.Gauge

	// PanicsTotal counts panics caught by the recovery middleware.
	PanicsTotal prometheus.Counter

	// CacheLookups counts every governor cache.Get call.
	// Labels: result = "hit" | "miss" | "error".
	CacheLookups *prometheus.CounterVec

	// CacheWrites counts every governor cache.Put call.
	// Labels: result = "ok" | "error" | "skipped" | "too_large".
	CacheWrites *prometheus.CounterVec

	// OptimizerRequests counts every optimization pass.
	// Labels: source = "optimize" | "stream".
	OptimizerRequests *prometheus.CounterVec

	// OptimizerBytes counts exact bytes seen before/after optimization.
	// Labels: source = "optimize" | "stream", stage = "before" | "after".
	OptimizerBytes *prometheus.CounterVec

	// OptimizerTokens counts estimated tokens seen before/after optimization.
	// Labels: source = "optimize" | "stream", stage = "before" | "after".
	OptimizerTokens *prometheus.CounterVec

	// OptimizerBlocks counts code-block outcomes.
	// Labels: source = "optimize" | "stream", result = "seen" | "pruned" | "skipped".
	OptimizerBlocks *prometheus.CounterVec

	// OptimizerDiffs counts exact vs fallback diff paths.
	// Labels: source = "optimize" | "stream", mode = "exact" | "fallback".
	OptimizerDiffs *prometheus.CounterVec

	// OptimizerSkips counts skipped optimization blocks by reason.
	// Labels: source = "optimize" | "stream", reason.
	OptimizerSkips *prometheus.CounterVec

	// OptimizerReduction captures byte reduction ratios from 0.0 to 1.0.
	// Labels: source = "optimize" | "stream".
	OptimizerReduction *prometheus.HistogramVec

	// FailoverRequests counts successful failover attempts.
	// Labels: from (failed provider), to (secondary provider).
	FailoverRequests *prometheus.CounterVec

	// EstimatedCostSavedUSD tracks cumulative dollars saved via pruning and caching.
	// Labels: provider, model.
	EstimatedCostSavedUSD *prometheus.CounterVec
}

func newMetrics(reg prometheus.Registerer) *Metrics {
	m := &Metrics{
		HTTPRequestsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "iq",
			Subsystem: "http",
			Name:      "requests_total",
			Help:      "Total HTTP requests handled, by method, route, and status.",
		}, []string{"method", "route", "status"}),

		HTTPDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "iq",
			Subsystem: "http",
			Name:      "request_duration_seconds",
			Help:      "Latency of HTTP requests including any streaming body time.",
			Buckets: []float64{
				0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25,
				0.5, 1, 2.5, 5, 10, 30, 60, 300,
			},
		}, []string{"method", "route"}),

		StreamsActive: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "iq",
			Subsystem: "stream",
			Name:      "active",
			Help:      "Number of streaming requests currently in flight.",
		}),

		PanicsTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "iq",
			Subsystem: "runtime",
			Name:      "panics_total",
			Help:      "Total panics caught by the recovery middleware.",
		}),

		CacheLookups: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "iq",
			Subsystem: "cache",
			Name:      "lookups_total",
			Help:      "Total cache lookups, by result (hit / miss / error).",
		}, []string{"result"}),

		CacheWrites: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "iq",
			Subsystem: "cache",
			Name:      "writes_total",
			Help:      "Total cache write attempts, by result (ok / error / skipped / too_large).",
		}, []string{"result"}),

		OptimizerRequests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "iq",
			Subsystem: "optimizer",
			Name:      "requests_total",
			Help:      "Total optimization passes by source.",
		}, []string{"source"}),

		OptimizerBytes: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "iq",
			Subsystem: "optimizer",
			Name:      "bytes_total",
			Help:      "Total exact bytes before and after optimization.",
		}, []string{"source", "stage"}),

		OptimizerTokens: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "iq",
			Subsystem: "optimizer",
			Name:      "tokens_total",
			Help:      "Total estimated tokens before and after optimization.",
		}, []string{"source", "stage"}),

		OptimizerBlocks: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "iq",
			Subsystem: "optimizer",
			Name:      "blocks_total",
			Help:      "Total optimizer code blocks by outcome.",
		}, []string{"source", "result"}),

		OptimizerDiffs: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "iq",
			Subsystem: "optimizer",
			Name:      "diffs_total",
			Help:      "Total optimizer diffs by exact or fallback path.",
		}, []string{"source", "mode"}),

		OptimizerSkips: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "iq",
			Subsystem: "optimizer",
			Name:      "skips_total",
			Help:      "Total skipped optimizer blocks by reason.",
		}, []string{"source", "reason"}),

		OptimizerReduction: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "iq",
			Subsystem: "optimizer",
			Name:      "reduction_ratio",
			Help:      "Observed byte reduction ratio for optimization passes.",
			Buckets:   []float64{0, 0.05, 0.1, 0.25, 0.5, 0.75, 0.9, 0.99, 1},
		}, []string{"source"}),

		FailoverRequests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "iq",
			Subsystem: "governor",
			Name:      "failover_requests_total",
			Help:      "Total successful failover attempts by from/to providers.",
		}, []string{"from", "to"}),

		EstimatedCostSavedUSD: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "iq",
			Subsystem: "finops",
			Name:      "cost_saved_usd",
			Help:      "Cumulative estimated cost saved in USD via context pruning and caching.",
		}, []string{"provider", "model"}),
	}

	reg.MustRegister(
		m.HTTPRequestsTotal,
		m.HTTPDuration,
		m.StreamsActive,
		m.PanicsTotal,
		m.CacheLookups,
		m.CacheWrites,
		m.OptimizerRequests,
		m.OptimizerBytes,
		m.OptimizerTokens,
		m.OptimizerBlocks,
		m.OptimizerDiffs,
		m.OptimizerSkips,
		m.OptimizerReduction,
		m.FailoverRequests,
		m.EstimatedCostSavedUSD,
	)

	// Pre-initialize label combinations the gateway will emit at runtime.
	// Counters with labels stay invisible to Prometheus scrapes until the
	// first Inc; pre-Adding a 0 makes them show up immediately so
	// dashboards and alerts have stable series from t=0.
	for _, r := range []string{"hit", "miss", "error"} {
		m.CacheLookups.WithLabelValues(r).Add(0)
	}
	for _, r := range []string{"ok", "error", "skipped", "too_large"} {
		m.CacheWrites.WithLabelValues(r).Add(0)
	}
	for _, source := range []string{"optimize", "stream"} {
		m.OptimizerRequests.WithLabelValues(source).Add(0)
		for _, stage := range []string{"before", "after"} {
			m.OptimizerBytes.WithLabelValues(source, stage).Add(0)
			m.OptimizerTokens.WithLabelValues(source, stage).Add(0)
		}
		for _, result := range []string{"seen", "pruned", "skipped"} {
			m.OptimizerBlocks.WithLabelValues(source, result).Add(0)
		}
		for _, mode := range []string{"exact", "fallback"} {
			m.OptimizerDiffs.WithLabelValues(source, mode).Add(0)
		}
		for _, reason := range []string{"too_many_lines", "empty", "not_smaller", "unknown"} {
			m.OptimizerSkips.WithLabelValues(source, reason).Add(0)
		}
		m.OptimizerReduction.WithLabelValues(source)
	}

	return m
}
