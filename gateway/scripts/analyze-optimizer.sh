#!/usr/bin/env bash
# analyze-optimizer.sh — deep-dive optimizer quality from gateway JSONL logs.
# Answers: where are the bytes? what was pruned? what was preserved and why?
# Which span class is the largest opportunity?
#
# Usage: ./analyze-optimizer.sh <gateway-log.jsonl> [more-log.jsonl ...]
set -euo pipefail

if ! command -v jq >/dev/null 2>&1; then
  printf 'jq is required.\n' >&2
  exit 1
fi

if [ "$#" -lt 1 ]; then
  printf 'usage: %s <gateway-log.jsonl> [more-log.jsonl ...]\n' "$0" >&2
  exit 2
fi

jq -R -s '
  split("\n")
  | map(fromjson? | select(.event == "request_complete")) as $events
  | $events
  | {
      summary: {
        requests: length,
        optimize_mode: (map(select(.mode == "optimize")) | length),
        total_bytes_before: (map(.bytes_before // 0) | add // 0),
        total_bytes_after: (map(.bytes_after // 0) | add // 0),
        total_bytes_eligible: (map(.bytes_eligible // 0) | add // 0),
        total_bytes_pruned: (map(.bytes_pruned // 0) | add // 0),
        total_tokens_saved: (map(.estimated_tokens_saved // 0) | add // 0),
        overall_prune_ratio: (
          if (map(.bytes_eligible // 0) | add // 0) > 0
          then ((map(.bytes_pruned // 0) | add // 0) / (map(.bytes_eligible // 0) | add // 0))
          else 0 end
        ),
        overall_reduction_ratio: (
          if (map(.bytes_before // 0) | add // 0) > 0
          then ((map(.bytes_before // 0) | add // 0) - (map(.bytes_after // 0) | add // 0)) /
               (map(.bytes_before // 0) | add // 0)
          else 0 end
        )
      },
      preserve_reasons: {
        latest_turn_bytes: (map(.preserved_latest_turn_bytes // 0) | add // 0),
        latest_turn_count: (map(.preserved_latest_turn_count // 0) | add // 0),
        small_span_count:  (map(.preserved_small_count // 0) | add // 0),
        system_text_count: (map(.preserved_system_count // 0) | add // 0),
        tool_use_count:    (map(.preserved_tool_use_count // 0) | add // 0)
      },
      span_sizes: {
        largest_seen:   (map(.largest_span_bytes // 0) | max // 0),
        largest_pruned: (map(.largest_pruned_bytes // 0) | max // 0),
        p95_input_bytes: (
          [.[].bytes_before // 0] | sort | .[ (length * 0.95 | floor) ] // 0
        )
      },
      class_breakdown: (
        [ $events[]
          | to_entries[]
          | select(.key | startswith("class_bytes_"))
          | { class: (.key | split(":")[1] // "unknown"),
              metric: (.key | split(":")[0] | ltrimstr("class_")),
              bytes: .value }
        ]
        | group_by(.class)
        | map({
            class: .[0].class,
            bytes_seen:     (map(select(.metric == "bytes_seen")     | .bytes) | add // 0),
            bytes_eligible: (map(select(.metric == "bytes_eligible") | .bytes) | add // 0),
            bytes_pruned:   (map(select(.metric == "bytes_pruned")   | .bytes) | add // 0),
            prune_ratio: (
              if (map(select(.metric == "bytes_eligible") | .bytes) | add // 0) > 0
              then (map(select(.metric == "bytes_pruned") | .bytes) | add // 0) /
                   (map(select(.metric == "bytes_eligible") | .bytes) | add // 0)
              else 0 end
            )
          })
        | sort_by(-.bytes_seen)
      ),
      top_savings_requests: (
        sort_by(.bytes_pruned // 0)
        | reverse
        | .[:10]
        | map({
            time,
            model,
            mode,
            bytes_before,
            bytes_pruned: (.bytes_pruned // 0),
            bytes_eligible: (.bytes_eligible // 0),
            tokens_saved: (.estimated_tokens_saved // 0),
            reduction_ratio,
            blocks_pruned,
            request_id
          })
      ),
      top_unpruned_eligible: (
        map(select((.bytes_eligible // 0) > 0 and (.bytes_pruned // 0) == 0))
        | sort_by(-.bytes_eligible)
        | .[:10]
        | map({
            time,
            model,
            mode,
            bytes_eligible: (.bytes_eligible // 0),
            preserved_latest_turn_bytes: (.preserved_latest_turn_bytes // 0),
            request_id
          })
      )
    }
' "$@"
