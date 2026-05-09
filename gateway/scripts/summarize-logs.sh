#!/usr/bin/env bash
set -euo pipefail

if ! command -v jq >/dev/null 2>&1; then
  printf 'jq is required to summarize gateway logs.\n' >&2
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
      requests: length,
      completed: (map(select(.status == "completed")) | length),
      errors: (map(select(.status != "completed")) | length),
      input_tokens: (map(.estimated_tokens_before // 0) | add // 0),
      after_tokens: (map(.estimated_tokens_after // 0) | add // 0),
      saved_tokens: (map(.estimated_tokens_saved // 0) | add // 0),
      avg_reduction: (if length > 0 then ((map(.reduction_ratio // 0) | add // 0) / length) else 0 end),
      max_input_tokens: (map(.estimated_tokens_before // 0) | max // 0),
      max_saved_tokens: (map(.estimated_tokens_saved // 0) | max // 0),
      by_status: (
        group_by(.status_code)
        | map({
            status_code: .[0].status_code,
            count: length,
            input_tokens: (map(.estimated_tokens_before // 0) | add // 0),
            saved_tokens: (map(.estimated_tokens_saved // 0) | add // 0)
          })
      ),
      by_model: (
        group_by(.model)
        | map({
            model: .[0].model,
            count: length,
            completed: (map(select(.status == "completed")) | length),
            errors: (map(select(.status != "completed")) | length),
            input_tokens: (map(.estimated_tokens_before // 0) | add // 0),
            saved_tokens: (map(.estimated_tokens_saved // 0) | add // 0),
            avg_reduction: (if length > 0 then ((map(.reduction_ratio // 0) | add // 0) / length) else 0 end)
          })
      ),
      top_input_requests: (
        sort_by(.estimated_tokens_before // 0)
        | reverse
        | .[:10]
        | map({
            time,
            status_code,
            model,
            input_tokens: .estimated_tokens_before,
            saved_tokens: .estimated_tokens_saved,
            reduction_ratio,
            blocks_seen,
            blocks_new,
            blocks_known,
            blocks_pruned,
            duration_ms,
            request_id
          })
      ),
      blocks: {
        total_seen: (map(.blocks_seen // 0) | add // 0),
        total_new: (map(.blocks_new // 0) | add // 0),
        total_known: (map(.blocks_known // 0) | add // 0),
        total_pruned: (map(.blocks_pruned // 0) | add // 0),
        known_ratio: (
          if (map(.blocks_seen // 0) | add // 0) > 0
          then ((map(.blocks_known // 0) | add // 0) / (map(.blocks_seen // 0) | add // 0))
          else 0 end
        )
      },
      optimizer_quality: {
        bytes_eligible: (map(.bytes_eligible // 0) | add // 0),
        bytes_pruned: (map(.bytes_pruned // 0) | add // 0),
        prune_ratio: (
          if (map(.bytes_eligible // 0) | add // 0) > 0
          then ((map(.bytes_pruned // 0) | add // 0) / (map(.bytes_eligible // 0) | add // 0))
          else 0 end
        ),
        preserved_latest_turn_bytes: (map(.preserved_latest_turn_bytes // 0) | add // 0),
        preserved_latest_turn_count: (map(.preserved_latest_turn_count // 0) | add // 0),
        preserved_small_count: (map(.preserved_small_count // 0) | add // 0),
        preserved_system_count: (map(.preserved_system_count // 0) | add // 0),
        preserved_tool_use_count: (map(.preserved_tool_use_count // 0) | add // 0),
        largest_span_bytes: (map(.largest_span_bytes // 0) | max // 0),
        largest_pruned_bytes: (map(.largest_pruned_bytes // 0) | max // 0),
        class_bytes_pruned: (
          map(to_entries | map(select(.key | startswith("class_bytes_pruned:")))
          | map({key: (.key | ltrimstr("class_bytes_pruned:")), value: .value}))
          | add // []
          | group_by(.key)
          | map({key: .[0].key, value: (map(.value) | add // 0)})
          | from_entries
        )
      }
    }
' "$@"
