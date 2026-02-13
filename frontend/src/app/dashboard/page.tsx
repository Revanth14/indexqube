"use client";

import { useEffect, useState } from "react";
import Link from "next/link";
import {
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  Area,
  AreaChart,
} from "recharts";
import { api, type IndexSummary, type IndexValuesResponse, type LiveMarket, type TopStock } from "@/lib/api";

const REFRESH_INTERVAL = 5_000;

function useLiveData<T>(fetcher: () => Promise<T>, deps: unknown[] = []) {
  const [data, setData] = useState<T | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const [refreshing, setRefreshing] = useState(false);
  const [lastUpdated, setLastUpdated] = useState<Date | null>(null);

  const refetch = async () => {
    if (!loading) setRefreshing(true);
    try {
      setError(null);
      const result = await fetcher();
      setData(result);
      setLastUpdated(new Date());
    } catch (e) {
      setError(e instanceof Error ? e.message : "Failed to fetch");
    } finally {
      setLoading(false);
      setRefreshing(false);
    }
  };

  useEffect(() => {
    refetch();
  }, deps);

  useEffect(() => {
    const id = setInterval(refetch, REFRESH_INTERVAL);
    return () => clearInterval(id);
  }, deps);

  return { data, error, loading, refreshing, lastUpdated, refetch };
}

function StatCard({
  label,
  value,
  sub,
}: {
  label: string;
  value: string | number;
  sub?: string;
}) {
  return (
    <div className="dash-stat">
      <span className="dash-stat-label">{label}</span>
      <span className="dash-stat-value">{value}</span>
      {sub && <span className="dash-stat-sub">{sub}</span>}
    </div>
  );
}

function useDashboardTimestamp(
  ...liveData: { lastUpdated: Date | null }[]
) {
  const latest = liveData
    .map((d) => d.lastUpdated)
    .filter(Boolean)
    .sort((a, b) => (b?.getTime() ?? 0) - (a?.getTime() ?? 0))[0];
  return latest ?? null;
}

export default function DashboardPage() {
  const { data: health, error: healthError, refreshing: healthRefreshing, lastUpdated: healthUpdated, refetch: refetchHealth } = useLiveData(api.health);
  const { data: liveMarkets, error: marketsError, lastUpdated: marketsUpdated, refetch: refetchLiveMarkets } = useLiveData(api.liveMarkets);
  const { data: top10Data, error: top10Error, lastUpdated: top10Updated, refetch: refetchLiveTop10 } = useLiveData(api.liveTop10);
  const { data: stats, refreshing: statsRefreshing, lastUpdated: statsUpdated, refetch: refetchStats } = useLiveData(api.stats);
  const { data: indices, refreshing: indicesRefreshing, lastUpdated: indicesUpdated, refetch: refetchIndices } = useLiveData(api.indices);
  const { data: validation, refreshing: validationRefreshing, lastUpdated: validationUpdated, refetch: refetchValidation } = useLiveData(api.validation);
  const [selectedTicker, setSelectedTicker] = useState<string | null>(null);

  const { data: indexValues, error: chartError, lastUpdated: chartUpdated } = useLiveData(
    async () => {
      if (!selectedTicker) return { ticker: "", count: 0, values: [] };
      return api.indexValues(selectedTicker);
    },
    [selectedTicker]
  );

  const lastUpdated = useDashboardTimestamp(
    { lastUpdated: healthUpdated },
    { lastUpdated: marketsUpdated },
    { lastUpdated: top10Updated },
    { lastUpdated: statsUpdated },
    { lastUpdated: indicesUpdated },
    { lastUpdated: validationUpdated },
    { lastUpdated: chartUpdated },
  );
  const isRefreshing = healthRefreshing || statsRefreshing || indicesRefreshing || validationRefreshing;

  const handleRefresh = () => {
    refetchHealth();
    refetchLiveMarkets();
    refetchLiveTop10();
    refetchStats();
    refetchIndices();
    refetchValidation();
  };

  const chartData = indexValues?.values ? [...indexValues.values].reverse() : [];

  const top10Stocks = top10Data?.stocks ?? [];

  return (
    <div className="page dash-page">
      <header className="topbar">
        <Link className="logo" href="/" aria-label="IndexQube home">
          <span className="logo-mark">IQ</span>
          IndexQube
        </Link>
        <nav className="nav">
          <Link href="/">Home</Link>
          <Link href="/dashboard" className="nav-active">
            Dashboard
          </Link>
        </nav>
      </header>

      <main className="dash-main">
        {/* Ticker tape */}
        <div className="dash-ticker">
          <div className="dash-ticker-inner">
            <div className="dash-ticker-track">
              <span>LIVE DATA</span>
              <span>·</span>
              <span>REAL-TIME METRICS</span>
              <span>·</span>
              <span>15-MIN DELAYED</span>
              <span>·</span>
              <span>PRODUCTION READY</span>
              <span>·</span>
              <span>MULTI-PROVIDER</span>
              <span>·</span>
              <span>INDEX INFRASTRUCTURE</span>
              <span>·</span>
            </div>
            <div className="dash-ticker-track" aria-hidden>
              <span>LIVE DATA</span>
              <span>·</span>
              <span>REAL-TIME METRICS</span>
              <span>·</span>
              <span>15-MIN DELAYED</span>
              <span>·</span>
              <span>PRODUCTION READY</span>
              <span>·</span>
              <span>MULTI-PROVIDER</span>
              <span>·</span>
              <span>INDEX INFRASTRUCTURE</span>
              <span>·</span>
            </div>
          </div>
        </div>

        <div className="dash-hero">
          <h1 className="dash-hero-title">
            <span className="dash-hero-gradient">Platform Dashboard</span>
          </h1>
          <p className="dash-hero-desc">
            Real-time index calculation infrastructure · Multi-provider validation · Audit-ready lineage
          </p>
        </div>

        <div className="dash-header">
          <div className="dash-header-row">
            <div>
              <p className="dash-subtitle">
                Live platform metrics · Auto-refresh every 5s
              </p>
            </div>
            <div className="dash-live-controls">
              <div className={`dash-live-badge ${isRefreshing ? "refreshing" : ""}`}>
                <span className="dash-live-pulse" />
                LIVE
              </div>
              {lastUpdated && (
                <span className="dash-last-updated">
                  Updated {lastUpdated.toLocaleTimeString()}
                </span>
              )}
              <button
                type="button"
                className="dash-refresh-btn"
                onClick={handleRefresh}
                disabled={isRefreshing}
                aria-label="Refresh data"
              >
                {isRefreshing ? (
                  <span className="dash-spinner" />
                ) : (
                  <span>↻ Refresh</span>
                )}
              </button>
            </div>
          </div>
          {healthError && (
            <div className="dash-api-offline">
              API backend not reachable. Start it with:{" "}
              <code>uvicorn api.main:app --reload</code>
            </div>
          )}
        </div>

        {/* Health */}
        <section className="dash-section dash-section-1">
          <div className="dash-section-header">
            <span className="dash-section-num">01</span>
            <h2>API Health</h2>
          </div>
          <div className="dash-health-grid">
            {healthError ? (
              <div className="dash-health-bad">
                <span className="dash-pulse" />
                Offline
                <span className="dash-err">{healthError}</span>
              </div>
            ) : health ? (
              <>
                <StatCard label="Status" value={health.status} />
                <StatCard label="Database" value={health.database} />
                <StatCard label="Version" value={health.version} />
                <div className="dash-health-ok">
                  <span className="dash-pulse ok" />
                  Connected
                </div>
              </>
            ) : (
              <div className="dash-loading">Loading...</div>
            )}
          </div>
        </section>

        {/* Live Markets - S&P 500 & NASDAQ */}
        <section className="dash-section dash-section-2">
          <div className="dash-section-header">
            <span className="dash-section-num">02</span>
            <h2>Market Benchmarks</h2>
          </div>
          <p className="dash-section-desc">15-minute delayed · via Polygon (Massive) API</p>
          {marketsError ? (
            <p className="dash-err">Failed to load market data. Check MASSIVE_API_KEY.</p>
          ) : liveMarkets?.markets?.length ? (
            <div className="dash-markets-grid">
              {liveMarkets.markets.map((m: LiveMarket) => (
                <div key={m.ticker} className="dash-market-card">
                  <div className="dash-market-header">
                    <strong>{m.ticker}</strong>
                    <span className="dash-market-index">{m.index}</span>
                  </div>
                  <p className="dash-market-name">{m.name}</p>
                  <div className="dash-market-price">
                    {m.price != null ? (
                      <>
                        ${m.price.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}
                        {m.change_pct != null && (
                          <span className={`dash-market-change ${m.change_pct >= 0 ? "pos" : "neg"}`}>
                            {m.change_pct >= 0 ? "+" : ""}{m.change_pct}%
                          </span>
                        )}
                      </>
                    ) : (
                      <span className="dash-loading">—</span>
                    )}
                  </div>
                  {m.source && (
                    <span className="dash-market-source">{m.source.replace("_", " ")}</span>
                  )}
                </div>
              ))}
            </div>
          ) : (
            <div className="dash-loading">Loading market data...</div>
          )}
        </section>

        {/* Top 10 US Stocks - 3rd tile */}
        <section className="dash-section dash-section-3">
          <div className="dash-section-header">
            <span className="dash-section-num">03</span>
            <h2>Top 10 US Stocks</h2>
          </div>
          <p className="dash-section-desc">15-minute delayed · via Polygon (Massive) API</p>
          {top10Error ? (
            <p className="dash-err">Failed to load stock data.</p>
          ) : top10Stocks.length > 0 ? (
            <div className="dash-top10-grid">
              {top10Stocks.map((s: TopStock) => (
                <div key={s.ticker} className="dash-top10-card">
                  <div className="dash-top10-header">
                    <strong>{s.ticker}</strong>
                    {s.change_pct != null && (
                      <span className={`dash-top10-change ${s.change_pct >= 0 ? "pos" : "neg"}`}>
                        {s.change_pct >= 0 ? "+" : ""}{s.change_pct}%
                      </span>
                    )}
                  </div>
                  <p className="dash-top10-name">{s.name}</p>
                  <div className="dash-top10-price">
                    {s.price != null ? (
                      `$${s.price.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`
                    ) : (
                      "—"
                    )}
                  </div>
                </div>
              ))}
            </div>
          ) : (
            <div className="dash-loading">Loading top stocks...</div>
          )}
        </section>

        {/* Platform Stats */}
        {stats && (
          <section className="dash-section dash-section-4">
            <div className="dash-section-header">
              <span className="dash-section-num">04</span>
              <h2>Platform Stats</h2>
            </div>
            <div className="dash-stats-grid">
              <StatCard
                label="Market data records"
                value={stats.market_data_records.toLocaleString()}
              />
              <StatCard
                label="Instruments tracked"
                value={stats.instruments_tracked.toLocaleString()}
              />
              <StatCard
                label="Index values"
                value={stats.index_values.toLocaleString()}
              />
              <StatCard
                label="Indices calculated"
                value={stats.indices_calculated}
              />
              <StatCard
                label="Corporate actions"
                value={stats.corporate_actions.toLocaleString()}
              />
              <StatCard
                label="Uptime"
                value={`${stats.uptime_pct}%`}
                sub="target"
              />
            </div>
          </section>
        )}

        {/* Indices */}
        {indices && indices.indices.length > 0 && (
          <section className="dash-section dash-section-5">
            <div className="dash-section-header">
              <span className="dash-section-num">05</span>
              <h2>Indices</h2>
            </div>
            <div className="dash-indices">
              {indices.indices.map((idx: IndexSummary) => (
                <button
                  key={idx.ticker}
                  type="button"
                  className={`dash-index-card ${selectedTicker === idx.ticker ? "selected" : ""}`}
                  onClick={() => setSelectedTicker(idx.ticker)}
                >
                  <div className="dash-index-header">
                    <strong>{idx.ticker}</strong>
                    <span
                      className={`dash-return ${idx.total_return_pct >= 0 ? "pos" : "neg"}`}
                    >
                      {idx.total_return_pct >= 0 ? "+" : ""}
                      {idx.total_return_pct}%
                    </span>
                  </div>
                  <p className="dash-index-name">{idx.name}</p>
                  <span className="dash-index-meta">
                    {idx.from_date} → {idx.to_date}
                  </span>
                </button>
              ))}
            </div>

            {selectedTicker && (
              <div className="dash-chart-wrap">
                <h3>{selectedTicker} performance</h3>
                {chartError ? (
                  <p className="dash-err">Failed to load chart data</p>
                ) : chartData.length > 0 ? (
                  <ResponsiveContainer width="100%" height={280}>
                    <AreaChart data={chartData} margin={{ top: 8, right: 8, left: 8, bottom: 8 }}>
                      <defs>
                        <linearGradient id="chartGrad" x1="0" y1="0" x2="0" y2="1">
                          <stop offset="0%" stopColor="var(--accent)" stopOpacity={0.4} />
                          <stop offset="100%" stopColor="var(--accent)" stopOpacity={0} />
                        </linearGradient>
                      </defs>
                      <CartesianGrid strokeDasharray="3 3" stroke="var(--line)" />
                      <XAxis
                        dataKey="date"
                        tickFormatter={(d) => d.slice(0, 7)}
                        fontSize={11}
                        stroke="var(--ink-soft)"
                      />
                      <YAxis
                        domain={["auto", "auto"]}
                        tickFormatter={(v) => v.toFixed(0)}
                        fontSize={11}
                        stroke="var(--ink-soft)"
                      />
                      <Tooltip
                        formatter={(v) => [typeof v === "number" ? v.toFixed(2) : String(v ?? ""), "Value"]}
                        labelFormatter={(l) => l}
                        contentStyle={{
                          background: "var(--surface)",
                          border: "1px solid var(--line)",
                          borderRadius: "12px",
                        }}
                      />
                      <Area
                        type="monotone"
                        dataKey="value"
                        stroke="var(--accent)"
                        strokeWidth={2}
                        fill="url(#chartGrad)"
                      />
                    </AreaChart>
                  </ResponsiveContainer>
                ) : (
                  <p className="dash-loading">Loading chart...</p>
                )}
              </div>
            )}
          </section>
        )}

        {/* Validation */}
        {validation && (
          <section className="dash-section dash-section-6">
            <div className="dash-section-header">
              <span className="dash-section-num">06</span>
              <h2>Validation Summary</h2>
            </div>
            <div className="dash-validation-grid">
              <StatCard
                label="Total checks"
                value={validation.total_checks.toLocaleString()}
              />
              {validation.provider_parity_score != null && (
                <StatCard
                  label="Provider parity score"
                  value={validation.provider_parity_score}
                />
              )}
              {validation.summary && validation.summary.length > 0 && (
              <div className="dash-validation-detail">
                <h4>By type & severity</h4>
                <div className="dash-validation-list">
                  {validation.summary.map((s) => (
                    <div key={`${s.type}-${s.severity}`} className="dash-validation-item">
                      <span>{s.type}</span>
                      <span className={`sev-${s.severity}`}>{s.severity}</span>
                      <span>{s.count}</span>
                    </div>
                  ))}
                </div>
              </div>
              )}
            </div>
          </section>
        )}

        <footer className="dash-footer">
          <p>IndexQube · Real-time index infrastructure for asset managers and index providers</p>
          <p className="dash-footer-built">Built with Next.js, FastAPI & Polygon</p>
        </footer>
      </main>
    </div>
  );
}
