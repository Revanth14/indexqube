const API_BASE = "/api";

async function fetchApi<T>(path: string): Promise<T> {
  const res = await fetch(`${API_BASE}${path}`);
  if (!res.ok) {
    throw new Error(`API error: ${res.status} ${res.statusText}`);
  }
  return res.json();
}

export type Health = {
  status: string;
  database: string;
  version: string;
};

export type IndexSummary = {
  ticker: string;
  name: string;
  type: string;
  description: string;
  data_points: number;
  from_date: string;
  to_date: string;
  total_return_pct: number;
  base_value: number;
};

export type IndicesResponse = {
  indices: IndexSummary[];
  count: number;
};

export type IndexDetail = {
  ticker: string;
  data_points: number;
  from_date: string;
  to_date: string;
  current_value: number;
  base_value: number;
  total_return_pct: number;
  annualized_return_pct: number;
  annualized_volatility_pct: number;
  sharpe_ratio: number;
  max_drawdown_pct: number;
  ytd_return_pct: number;
};

export type IndexValue = {
  date: string;
  value: number;
  return_pct: number;
};

export type IndexValuesResponse = {
  ticker: string;
  count: number;
  values: IndexValue[];
};

export type Stats = {
  market_data_records: number;
  instruments_tracked: number;
  corporate_actions: number;
  index_values: number;
  indices_calculated: number;
  data_providers: number;
  uptime_pct: number;
};

export type ValidationItem = {
  type: string;
  severity: string;
  count: number;
};

export type ValidationSummary = {
  summary: ValidationItem[];
  provider_parity_score: number | null;
  total_checks: number;
};

export type LiveMarket = {
  ticker: string;
  name: string;
  index: string;
  price: number | null;
  change_pct: number | null;
  volume: number | null;
  updated_at: string | null;
  source: string | null;
};

export type LiveMarketsResponse = {
  markets: LiveMarket[];
};

export type TopStock = {
  ticker: string;
  name: string;
  price: number | null;
  change_pct: number | null;
  source: string | null;
};

export type Top10StocksResponse = {
  stocks: TopStock[];
};

export const api = {
  health: () => fetchApi<Health>("/health"),
  stats: () => fetchApi<Stats>("/stats"),
  indices: () => fetchApi<IndicesResponse>("/indices"),
  index: (ticker: string) => fetchApi<IndexDetail>(`/indices/${ticker}`),
  indexValues: (ticker: string, limit = 252) =>
    fetchApi<IndexValuesResponse>(`/indices/${ticker}/values?limit=${limit}`),
  validation: () => fetchApi<ValidationSummary>("/validation/summary"),
  liveMarkets: () => fetchApi<LiveMarketsResponse>("/live/markets"),
  liveTop10: () => fetchApi<Top10StocksResponse>("/live/markets/top10"),
};
