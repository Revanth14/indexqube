#!/usr/bin/env python3
"""IndexQube Session Audit Tool — production-grade CLI for analyzing LLM proxy traces.

Reads session JSONL traces emitted by the IndexQube L7 gateway, builds a
compressed analysis prompt, streams it through AWS Bedrock, and persists
the structured JSON report locally, optionally to S3 and CloudWatch.

Usage:
    python3 scripts/audit_session.py                     # audit latest trace
    python3 scripts/audit_session.py --session <id>      # audit specific session
    python3 scripts/audit_session.py list                 # list available traces
    python3 scripts/audit_session.py stats                # quick local stats (no Bedrock)
"""
from __future__ import annotations

import argparse
import dataclasses
import glob
import json
import logging
import os
import re
import shutil
import sys
import textwrap
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any, Optional, Sequence

# ──────────────────────────────────────────────────────────────────────
# Logging
# ──────────────────────────────────────────────────────────────────────
LOG_FORMAT = "%(asctime)s [%(levelname)s] %(message)s"
LOG_DATEFMT = "%H:%M:%S"

logger = logging.getLogger("iq-audit")


# ──────────────────────────────────────────────────────────────────────
# Terminal colors (auto-disabled when piped / NO_COLOR)
# ──────────────────────────────────────────────────────────────────────
class _Colors:
    """ANSI escape wrapper that silences itself when stdout is not a tty."""

    _enabled: bool = sys.stdout.isatty() and os.environ.get("NO_COLOR") is None

    BLUE = "\033[1;36m"
    GREEN = "\033[1;32m"
    YELLOW = "\033[1;33m"
    RED = "\033[1;31m"
    DIM = "\033[2m"
    BOLD = "\033[1m"
    RESET = "\033[0m"

    def __getattr__(self, name: str) -> str:
        val = object.__getattribute__(self, name) if name.isupper() else ""
        return val if self._enabled else ""


C = _Colors()


# ──────────────────────────────────────────────────────────────────────
# Configuration (env-overridable)
# ──────────────────────────────────────────────────────────────────────
@dataclass(frozen=True)
class Config:
    """Immutable runtime configuration."""

    # Dump directories — searched in order
    dump_search_paths: tuple[str, ...] = (
        ".indexqube/dumps",
        "gateway/.indexqube/dumps",
        "~/.indexqube/dumps",
    )
    # Housekeeping
    max_retained_traces: int = 30

    # Bedrock
    bedrock_region: str = os.environ.get("IQ_BEDROCK_REGION", "us-east-1")
    bedrock_connect_timeout: int = 10
    bedrock_read_timeout: int = 90
    bedrock_max_retries: int = 2

    # Model fallback chain
    model_ids: tuple[str, ...] = (
        "us.anthropic.claude-opus-4-6-v1",
        "anthropic.claude-opus-4-6-v1",
        "us.anthropic.claude-sonnet-4-6",
        "anthropic.claude-sonnet-4-6",
        "amazon.nova-pro-v1:0",
    )

    # Prompt budget
    max_audit_turns: int = 18
    prompt_token_hard_cap: int = 150_000     # ~600 KB chars
    text_truncate_threshold: int = 5120      # bytes before truncation kicks in
    text_truncate_retain: int = 4000          # bytes kept after truncation

    # Outputs
    s3_bucket: str = os.environ.get("IQ_S3_BUCKET", "indexqube-session-traces")
    cw_namespace: str = "IndexQube"
    enable_s3: bool = os.environ.get("IQ_ENABLE_S3", "1") == "1"
    enable_cloudwatch: bool = os.environ.get("IQ_ENABLE_CW", "1") == "1"


CFG = Config()

# ──────────────────────────────────────────────────────────────────────
# Data models
# ──────────────────────────────────────────────────────────────────────

class ResponseStatus(str, Enum):
    COMPLETED = "completed"
    ERROR = "error"
    UNKNOWN = "unknown"


@dataclass
class OptimizerMetrics:
    """Per-turn optimizer stats emitted by the Go gateway."""

    blocks_pruned: int = 0
    blocks_known: int = 0
    blocks_known_protected: int = 0
    bytes_pruned: int = 0
    protected_bytes: int = 0
    known_bytes: int = 0
    true_cache_hit_bytes: int = 0

    @classmethod
    def from_record(cls, raw: dict[str, Any]) -> OptimizerMetrics:
        opt = raw.get("optimizer") or {}
        saved = raw.get("saved_bytes", 0)
        return cls(
            blocks_pruned=opt.get("blocks_pruned", 0),
            blocks_known=opt.get("blocks_known", 0),
            blocks_known_protected=opt.get("blocks_known_protected", 0),
            bytes_pruned=opt.get("bytes_pruned", saved),
            protected_bytes=opt.get("protected_bytes", 0),
            known_bytes=opt.get("known_bytes", 0),
            true_cache_hit_bytes=opt.get("true_cache_hit_bytes", saved),
        )


@dataclass
class TurnRecord:
    """Normalized representation of one proxy turn."""

    index: int                         # 1-based turn number
    ts: str = ""
    request_id: str = ""
    before_bytes: int = 0
    after_bytes: int = 0
    saved_bytes: int = 0
    model: str = ""
    status: ResponseStatus = ResponseStatus.UNKNOWN
    output_tokens: int = 0
    optimizer: OptimizerMetrics = field(default_factory=OptimizerMetrics)
    # Raw payloads kept for prompt construction (not serialized to reports)
    _before_payload: dict = field(default_factory=dict, repr=False)
    _response_text: str = field(default="", repr=False)

    @property
    def reduction_pct(self) -> float:
        return (self.saved_bytes / self.before_bytes * 100) if self.before_bytes > 0 else 0.0

    @classmethod
    def from_raw(cls, raw: dict[str, Any], index: int) -> TurnRecord:
        before = raw.get("before") or {}
        resp = raw.get("response") or {}
        status_str = resp.get("status", "unknown")
        try:
            status = ResponseStatus(status_str)
        except ValueError:
            status = ResponseStatus.UNKNOWN
        return cls(
            index=index,
            ts=raw.get("ts", ""),
            request_id=raw.get("request_id", ""),
            before_bytes=raw.get("before_bytes", 0),
            after_bytes=raw.get("after_bytes", 0),
            saved_bytes=raw.get("saved_bytes", 0),
            model=before.get("model", "unknown"),
            status=status,
            output_tokens=resp.get("output_tokens", 0),
            optimizer=OptimizerMetrics.from_record(raw),
            _before_payload=before,
            _response_text=resp.get("text", ""),
        )

    def summary_dict(self) -> dict[str, Any]:
        """Serializable summary for stats output."""
        return {
            "turn": self.index,
            "ts": self.ts,
            "model": self.model,
            "before_bytes": self.before_bytes,
            "after_bytes": self.after_bytes,
            "saved_bytes": self.saved_bytes,
            "reduction_pct": round(self.reduction_pct, 1),
            "status": self.status.value,
            "output_tokens": self.output_tokens,
            "optimizer": dataclasses.asdict(self.optimizer),
        }


@dataclass
class SessionTrace:
    """Parsed and validated session trace."""

    session_id: str
    file_path: str
    turns: list[TurnRecord]
    parse_warnings: int = 0

    @property
    def total_before(self) -> int:
        return sum(t.before_bytes for t in self.turns)

    @property
    def total_saved(self) -> int:
        return sum(t.saved_bytes for t in self.turns)

    @property
    def total_reduction_pct(self) -> float:
        return (self.total_saved / self.total_before * 100) if self.total_before > 0 else 0.0

    @property
    def models_used(self) -> set[str]:
        return {t.model for t in self.turns}

    @property
    def error_count(self) -> int:
        return sum(1 for t in self.turns if t.status == ResponseStatus.ERROR)


# ──────────────────────────────────────────────────────────────────────
# Dump directory & file resolution
# ──────────────────────────────────────────────────────────────────────

def _resolve_dumps_dir() -> Path | None:
    """Find the first existing dumps directory from the search path."""
    for rel in CFG.dump_search_paths:
        p = Path(os.path.expanduser(rel))
        if not p.is_absolute():
            p = Path.cwd() / p
        if p.is_dir():
            return p
    return None


def _list_trace_files(include_audited: bool = False) -> list[Path]:
    """Return trace files sorted newest-first by mtime."""
    dumps = _resolve_dumps_dir()
    if dumps is None:
        return []
    files = list(dumps.glob("iq-session-*.jsonl"))
    if include_audited:
        files.extend((dumps / "audited").glob("iq-session-*.jsonl"))
    files.sort(key=lambda f: f.stat().st_mtime, reverse=True)
    return files


def _resolve_session_file(session_arg: str | None) -> Path | None:
    """Resolve a session file from CLI argument or auto-detect latest."""
    if session_arg is None:
        files = _list_trace_files(include_audited=False)
        return files[0] if files else None

    # Direct path
    candidate = Path(session_arg)
    if candidate.is_file():
        return candidate

    # Try resolving as session ID
    dumps = _resolve_dumps_dir()
    if dumps is None:
        return None
    for subdir in [dumps, dumps / "audited"]:
        f = subdir / f"iq-session-{session_arg}.jsonl"
        if f.is_file():
            return f
    return None


# ──────────────────────────────────────────────────────────────────────
# Housekeeping
# ──────────────────────────────────────────────────────────────────────

def auto_rotate_traces() -> int:
    """Delete oldest traces beyond retention limit. Returns count deleted."""
    files = _list_trace_files(include_audited=True)
    if len(files) <= CFG.max_retained_traces:
        return 0
    to_delete = files[CFG.max_retained_traces:]
    deleted = 0
    for f in to_delete:
        try:
            f.unlink()
            deleted += 1
        except OSError as e:
            logger.warning("Failed to remove old trace %s: %s", f.name, e)
    return deleted


# ──────────────────────────────────────────────────────────────────────
# Trace parsing
# ──────────────────────────────────────────────────────────────────────

def parse_trace(filepath: Path) -> SessionTrace:
    """Parse a JSONL session trace file into a SessionTrace object."""
    session_id = filepath.stem.removeprefix("iq-session-")
    turns: list[TurnRecord] = []
    warnings = 0

    with filepath.open("r", encoding="utf-8") as fh:
        for lineno, line in enumerate(fh, start=1):
            stripped = line.strip()
            if not stripped:
                continue
            try:
                raw = json.loads(stripped)
            except json.JSONDecodeError:
                warnings += 1
                logger.warning("Skipped malformed JSON at %s:%d", filepath.name, lineno)
                continue
            turns.append(TurnRecord.from_raw(raw, index=len(turns) + 1))

    return SessionTrace(
        session_id=session_id,
        file_path=str(filepath),
        turns=turns,
        parse_warnings=warnings,
    )


# ──────────────────────────────────────────────────────────────────────
# Text compression for prompt construction
# ──────────────────────────────────────────────────────────────────────

def _truncate(text: str, retain: int = CFG.text_truncate_retain, threshold: int = CFG.text_truncate_threshold) -> str:
    """Smart truncation preserving head and tail."""
    if len(text) <= threshold:
        return text
    half = retain // 2
    omitted = len(text) - retain
    return f"{text[:half]}\n\n[... TRUNCATED {omitted:,} bytes ...]\n\n{text[-half:]}"


def _compress_content(content: Any) -> str:
    """Compress message content blocks for prompt inclusion."""
    if not content:
        return ""
    if isinstance(content, str):
        return _truncate(content)
    if isinstance(content, list):
        parts = []
        for block in content:
            if not isinstance(block, dict):
                parts.append(str(block))
                continue
            btype = block.get("type", "")
            if btype == "text":
                parts.append(_truncate(block.get("text", ""), retain=3000))
            elif btype == "tool_result":
                parts.append(f"[tool_result] {_truncate(str(block.get('content', '')), retain=2000)}")
            elif btype == "tool_use":
                parts.append(f"[tool_use:{block.get('name', '?')}] input={_truncate(str(block.get('input', '')), retain=1500)}")
            else:
                parts.append(f"[{btype}] {_truncate(str(block), retain=1000)}")
        return "\n".join(parts)
    return _truncate(str(content))


# ──────────────────────────────────────────────────────────────────────
# Prompt construction
# ──────────────────────────────────────────────────────────────────────

_SYSTEM_INSTRUCTION = textwrap.dedent("""\
    You are an expert compiler and LLM systems engineer specializing in token
    deduplication, sliding-window Rabin-Karp chunking, and database caching
    strategies.

    Analyze the provided IndexQube proxy session trace and return a VALID,
    STRICT JSON object. Do NOT output any conversational text, code fences,
    or Markdown outside the JSON itself.

    Schema:
    {
      "summary": "High-level session analysis (2-4 sentences).",
      "cache_eviction_loopholes": ["..."],
      "chunking_inefficiencies": ["..."],
      "loop_breaker_bypasses": ["..."],
      "anomalies": ["Any request-ID collisions, duplicate payloads, or timing oddities"],
      "actionable_recommendations": [
        {"target": "package/file", "change": "Specific fix", "impact": "Expected improvement", "priority": "P0|P1|P2"}
      ],
      "session_health_score": 0-100
    }
""")


def _select_audit_turns(trace: SessionTrace) -> list[TurnRecord]:
    """Select the most informative turns for the audit prompt.

    Strategy:
    - Always include first 3 turns (context warm-up visibility)
    - Always include last 3 turns (most recent behavior)
    - Fill remaining slots (up to max_audit_turns) with the
      lowest-savings middle turns (highest optimization opportunity)
    """
    n = len(trace.turns)
    cap = CFG.max_audit_turns

    if n <= cap:
        return list(trace.turns)

    keep_indices: set[int] = set()
    # First 3
    for i in range(min(3, n)):
        keep_indices.add(i)
    # Last 3
    for i in range(max(0, n - 3), n):
        keep_indices.add(i)

    # Middle: sort by lowest true_cache_hit_bytes (highest signal)
    middle = [i for i in range(n) if i not in keep_indices]
    middle.sort(key=lambda i: (
        trace.turns[i].optimizer.true_cache_hit_bytes,
        trace.turns[i].before_bytes,
    ))

    remaining = cap - len(keep_indices)
    if remaining > 0:
        keep_indices.update(middle[:remaining])

    selected = sorted(keep_indices)
    return [trace.turns[i] for i in selected]


def build_audit_prompt(trace: SessionTrace) -> str:
    """Construct the full audit prompt from a session trace."""
    selected = _select_audit_turns(trace)

    header = (
        f"Analyze the following IndexQube L7 proxy session ({len(trace.turns)} total turns, "
        f"{len(selected)} shown). Each turn shows the payload before/after chunk pruning and "
        f"the model response.\n\n"
        f"Session ID: {trace.session_id}\n"
        f"Models used: {', '.join(trace.models_used)}\n"
        f"Total input bytes: {trace.total_before:,}\n"
        f"Total bytes saved: {trace.total_saved:,} ({trace.total_reduction_pct:.1f}%)\n"
        f"Error turns: {trace.error_count}\n\n"
        "Identify:\n"
        "1. Cache eviction loopholes — large misses that should have hit\n"
        "2. Chunking inefficiencies — suboptimal block grouping or Rabin-Karp window sizing\n"
        "3. Loop/circuit-breaker bypasses — repeated prompts or synthetic ID collisions\n"
        "4. Anomalies — duplicate request IDs, identical payloads, timing clusters\n"
        "5. Actionable codebase improvements with priority ranking\n\n"
        "--- SESSION EXECUTION TRACES ---\n"
    )

    parts = [header]
    for turn in selected:
        opt = turn.optimizer
        block = (
            f"\n[TURN {turn.index}/{len(trace.turns)}] ts={turn.ts}\n"
            f"  request_id: {turn.request_id}\n"
            f"  model: {turn.model} | status: {turn.status.value} | output_tokens: {turn.output_tokens}\n"
            f"  bytes: before={turn.before_bytes:,} after={turn.after_bytes:,} "
            f"saved={turn.saved_bytes:,} ({turn.reduction_pct:.1f}%)\n"
            f"  optimizer: pruned={opt.bytes_pruned:,} protected={opt.protected_bytes:,} "
            f"known={opt.known_bytes:,} cache_hit={opt.true_cache_hit_bytes:,} "
            f"blocks_pruned={opt.blocks_pruned} blocks_known={opt.blocks_known}\n"
        )

        # Turn 1: include system prompt + full messages for context setup visibility
        if turn.index == 1:
            sys_prompt = turn._before_payload.get("system", "")
            if sys_prompt:
                block += f"  system_prompt:\n{textwrap.indent(_compress_content(sys_prompt), '    ')}\n"
            msgs = turn._before_payload.get("messages", [])
            if msgs:
                block += f"  full_context:\n{textwrap.indent(_compress_content(msgs), '    ')}\n"
        else:
            # Delta: only the latest user message
            msgs = turn._before_payload.get("messages", [])
            if msgs:
                last = msgs[-1]
                content = last.get("content", "")
                block += f"  latest_prompt:\n{textwrap.indent(_compress_content(content), '    ')}\n"

        # Response preview
        if turn._response_text:
            block += f"  response_preview:\n{textwrap.indent(_truncate(turn._response_text, retain=2000), '    ')}\n"

        block += "  " + "─" * 60 + "\n"
        parts.append(block)

    prompt = "".join(parts)

    # Hard cap guard
    char_limit = CFG.prompt_token_hard_cap * 4
    if len(prompt) > char_limit:
        logger.warning(
            "Prompt exceeds token cap (~%dk tokens). Truncating to %dk chars.",
            len(prompt) // 4000, char_limit // 1000,
        )
        prompt = prompt[:char_limit]

    return prompt


# ──────────────────────────────────────────────────────────────────────
# AWS helpers
# ──────────────────────────────────────────────────────────────────────

def _get_boto3():
    """Lazy-import boto3 with a clear error if missing."""
    try:
        import boto3
        from botocore.config import Config as BotoConfig
        return boto3, BotoConfig
    except ImportError:
        logger.error(
            "boto3 is not installed. Run: pip install boto3"
        )
        sys.exit(1)


def _verify_aws_creds() -> bool:
    """Fast STS identity check."""
    boto3, _ = _get_boto3()
    try:
        boto3.client("sts").get_caller_identity()
        return True
    except Exception:
        return False


# ──────────────────────────────────────────────────────────────────────
# Bedrock streaming audit
# ──────────────────────────────────────────────────────────────────────

def _parse_bedrock_response(raw: str) -> dict[str, Any]:
    """Strip markdown fences and parse JSON from Bedrock response."""
    text = raw.strip()
    # Remove ```json ... ``` wrapping if present
    fence_pattern = re.compile(r"^```(?:json)?\s*\n?(.*?)\n?\s*```$", re.DOTALL)
    m = fence_pattern.match(text)
    if m:
        text = m.group(1).strip()
    return json.loads(text)


def run_bedrock_audit(prompt: str) -> dict[str, Any] | None:
    """Stream an audit through Bedrock and return the parsed JSON report."""
    boto3, BotoConfig = _get_boto3()

    if not _verify_aws_creds():
        print(f"\n  {C.RED}✗ AWS credentials expired or missing.{C.RESET}")
        print(f"    Run: {C.GREEN}aws sso login{C.RESET}\n")
        return None

    config = BotoConfig(
        region_name=CFG.bedrock_region,
        connect_timeout=CFG.bedrock_connect_timeout,
        read_timeout=CFG.bedrock_read_timeout,
        retries={"max_attempts": CFG.bedrock_max_retries},
    )

    try:
        client = boto3.client("bedrock-runtime", config=config)
    except Exception as e:
        logger.error("Failed to initialize Bedrock client: %s", e)
        return None

    body = {
        "anthropic_version": "bedrock-2023-05-31",
        "max_tokens": 4096,
        "temperature": 0.1,
        "system": _SYSTEM_INSTRUCTION,
        "messages": [{"role": "user", "content": prompt}],
    }

    for model_id in CFG.model_ids:
        try:
            print(f"  {C.DIM}Trying model:{C.RESET} {C.YELLOW}{model_id}{C.RESET} ", end="", flush=True)
            response = client.invoke_model_with_response_stream(
                modelId=model_id,
                contentType="application/json",
                accept="application/json",
                body=json.dumps(body),
            )
            print(f"{C.GREEN}✓{C.RESET}")
            break
        except Exception as e:
            err = str(e)
            if "AccessDenied" in err or "ValidationException" in err:
                print(f"{C.RED}✗{C.RESET}")
                logger.debug("Model %s denied: %s", model_id, err[:120])
            else:
                print(f"{C.YELLOW}⚠{C.RESET}")
                logger.debug("Model %s failed: %s", model_id, err[:120])
    else:
        print(f"\n  {C.RED}✗ All models failed. Check Bedrock access / region.{C.RESET}\n")
        return None

    # Stream response
    print(f"\n{C.GREEN}{'━' * 64}{C.RESET}")
    print(f"{C.BOLD}  IndexQube Optimization Report (streaming){C.RESET}")
    print(f"{C.GREEN}{'━' * 64}{C.RESET}\n")

    payload = []
    stream = response.get("body")
    for event in stream:
        chunk_bytes = event.get("chunk", {}).get("bytes", b"")
        if not chunk_bytes:
            continue
        try:
            chunk = json.loads(chunk_bytes.decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError):
            continue
        if chunk.get("type") == "content_block_delta":
            text = chunk.get("delta", {}).get("text", "")
            payload.append(text)
            print(text, end="", flush=True)

    print(f"\n\n{C.GREEN}{'━' * 64}{C.RESET}\n")

    raw_text = "".join(payload)
    try:
        report = _parse_bedrock_response(raw_text)
        print(f"  {C.GREEN}✓ Parsed structured JSON report successfully{C.RESET}")
        return report
    except (json.JSONDecodeError, ValueError) as e:
        logger.warning("Response was not valid JSON: %s", e)
        return {"summary": raw_text, "_parse_error": str(e)}


# ──────────────────────────────────────────────────────────────────────
# Post-audit actions (S3, CloudWatch, archival)
# ──────────────────────────────────────────────────────────────────────

def _upload_to_s3(filepath: str, session_id: str) -> None:
    """Upload trace to S3 for long-term archival."""
    if not CFG.enable_s3:
        return
    boto3, _ = _get_boto3()
    try:
        s3 = boto3.client("s3")
        key = f"traces/{session_id}.jsonl"
        s3.upload_file(filepath, CFG.s3_bucket, key)
        print(f"  {C.GREEN}✓{C.RESET} Archived to {C.DIM}s3://{CFG.s3_bucket}/{key}{C.RESET}")
    except Exception as e:
        logger.warning("S3 upload failed (non-fatal): %s", e)


def _emit_cloudwatch(trace: SessionTrace) -> None:
    """Publish optimization metrics to CloudWatch."""
    if not CFG.enable_cloudwatch:
        return
    boto3, _ = _get_boto3()
    try:
        cw = boto3.client("cloudwatch")
        dims = [{"Name": "SessionID", "Value": trace.session_id}]
        cw.put_metric_data(
            Namespace=CFG.cw_namespace,
            MetricData=[
                {"MetricName": "TotalBytesSaved", "Value": trace.total_saved, "Unit": "Bytes", "Dimensions": dims},
                {"MetricName": "ReductionPercent", "Value": trace.total_reduction_pct, "Unit": "Percent", "Dimensions": dims},
                {"MetricName": "TurnCount", "Value": len(trace.turns), "Unit": "Count", "Dimensions": dims},
                {"MetricName": "ErrorTurns", "Value": trace.error_count, "Unit": "Count", "Dimensions": dims},
            ],
        )
        print(f"  {C.GREEN}✓{C.RESET} Metrics emitted to {C.DIM}CloudWatch:{CFG.cw_namespace}{C.RESET}")
    except Exception as e:
        logger.warning("CloudWatch emit failed (non-fatal): %s", e)


def _save_report(report: dict[str, Any], session_id: str) -> Path | None:
    """Persist JSON report to local filesystem."""
    dumps = _resolve_dumps_dir()
    if dumps is None:
        logger.warning("No dumps directory found — skipping report save")
        return None
    reports_dir = dumps / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)
    path = reports_dir / f"audit-{session_id}.json"
    with path.open("w", encoding="utf-8") as fh:
        json.dump(report, fh, indent=2, ensure_ascii=False)
    print(f"  {C.GREEN}✓{C.RESET} Report saved to {C.YELLOW}{path}{C.RESET}")
    return path


def _archive_trace(filepath: str) -> None:
    """Move audited trace into the audited/ subdirectory."""
    src = Path(filepath)
    dest_dir = src.parent / "audited"
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest = dest_dir / src.name
    try:
        shutil.move(str(src), str(dest))
        print(f"  {C.GREEN}✓{C.RESET} Trace archived to {C.DIM}{dest}{C.RESET}")
    except OSError as e:
        logger.warning("Failed to archive trace: %s", e)


def run_post_audit(trace: SessionTrace, report: dict[str, Any]) -> None:
    """Execute all post-audit side-effects concurrently where possible."""
    _save_report(report, trace.session_id)

    # S3 + CloudWatch can run in parallel
    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = []
        if CFG.enable_s3:
            futures.append(pool.submit(_upload_to_s3, trace.file_path, trace.session_id))
        if CFG.enable_cloudwatch:
            futures.append(pool.submit(_emit_cloudwatch, trace))
        for f in as_completed(futures):
            try:
                f.result()
            except Exception as e:
                logger.warning("Post-audit task failed: %s", e)

    _archive_trace(trace.file_path)


# ──────────────────────────────────────────────────────────────────────
# CLI: stats subcommand (no Bedrock, local only)
# ──────────────────────────────────────────────────────────────────────

def _fmt_bytes(n: int) -> str:
    """Human-readable byte count."""
    if n < 1024:
        return f"{n} B"
    elif n < 1024 ** 2:
        return f"{n / 1024:.1f} KB"
    else:
        return f"{n / (1024 ** 2):.2f} MB"


def _print_table(headers: list[str], rows: list[list[str]], col_align: list[str] | None = None) -> None:
    """Print a formatted ASCII table."""
    widths = [len(h) for h in headers]
    for row in rows:
        for i, cell in enumerate(row):
            widths[i] = max(widths[i], len(cell))

    if col_align is None:
        col_align = ["<"] * len(headers)

    sep = "─"
    header_line = "  ".join(f"{h:{a}{w}}" for h, w, a in zip(headers, widths, col_align))
    divider = "  ".join(sep * w for w in widths)

    print(f"  {C.BOLD}{header_line}{C.RESET}")
    print(f"  {C.DIM}{divider}{C.RESET}")
    for row in rows:
        line = "  ".join(f"{cell:{a}{w}}" for cell, w, a in zip(row, widths, col_align))
        print(f"  {line}")


def cmd_stats(args: argparse.Namespace) -> int:
    """Print local statistics for a session trace without calling Bedrock."""
    filepath = _resolve_session_file(args.session)
    if filepath is None:
        print(f"  {C.RED}No session traces found.{C.RESET}")
        return 1

    trace = parse_trace(filepath)
    print(f"  Session: {C.YELLOW}{trace.session_id}{C.RESET}")
    print(f"  Turns:   {C.GREEN}{len(trace.turns)}{C.RESET}  |  "
          f"Errors: {C.RED if trace.error_count else C.GREEN}{trace.error_count}{C.RESET}  |  "
          f"Models: {', '.join(trace.models_used)}")
    print(f"  Total:   {_fmt_bytes(trace.total_before)} → {_fmt_bytes(trace.total_before - trace.total_saved)} "
          f"({C.GREEN}{trace.total_reduction_pct:.1f}%{C.RESET} reduction)\n")

    if trace.parse_warnings:
        print(f"  {C.YELLOW}⚠ {trace.parse_warnings} malformed lines skipped{C.RESET}\n")

    # Per-turn table
    headers = ["Turn", "Model", "Before", "After", "Saved", "%", "Status", "Pruned", "Cache Hit"]
    rows = []
    for t in trace.turns:
        pct_str = f"{t.reduction_pct:.1f}%"
        status_str = "✓" if t.status == ResponseStatus.COMPLETED else ("✗" if t.status == ResponseStatus.ERROR else "?")
        rows.append([
            str(t.index),
            t.model[:20],
            _fmt_bytes(t.before_bytes),
            _fmt_bytes(t.after_bytes),
            _fmt_bytes(t.saved_bytes),
            pct_str,
            status_str,
            _fmt_bytes(t.optimizer.bytes_pruned),
            _fmt_bytes(t.optimizer.true_cache_hit_bytes),
        ])

    _print_table(headers, rows, col_align=[">"]*1 + ["<"]*1 + [">"]*4 + ["^"]*1 + [">"]*2)

    # Anomaly detection
    print(f"\n  {C.BOLD}Anomaly Scan{C.RESET}")
    anomalies = _detect_anomalies(trace)
    if anomalies:
        for a in anomalies:
            print(f"    {C.YELLOW}⚠{C.RESET} {a}")
    else:
        print(f"    {C.GREEN}✓ No anomalies detected{C.RESET}")

    return 0


def _detect_anomalies(trace: SessionTrace) -> list[str]:
    """Run local heuristic anomaly checks."""
    issues = []

    # Duplicate request IDs
    rid_counts: dict[str, int] = {}
    for t in trace.turns:
        rid_counts[t.request_id] = rid_counts.get(t.request_id, 0) + 1
    dupes = {k: v for k, v in rid_counts.items() if v > 1}
    if dupes:
        issues.append(f"Duplicate request IDs: {len(dupes)} IDs reused across {sum(dupes.values())} turns")

    # Consecutive zero-savings turns after warm-up
    zero_streak = 0
    max_zero_streak = 0
    for t in trace.turns[3:]:  # skip first 3 (cold start)
        if t.saved_bytes == 0 and t.before_bytes > 1000:
            zero_streak += 1
            max_zero_streak = max(max_zero_streak, zero_streak)
        else:
            zero_streak = 0
    if max_zero_streak >= 5:
        issues.append(f"Long zero-savings streak: {max_zero_streak} consecutive turns with no dedup after warm-up")

    # Rapid-fire identical payloads
    for i in range(1, len(trace.turns)):
        curr = trace.turns[i]
        prev = trace.turns[i - 1]
        if (curr.before_bytes == prev.before_bytes
                and curr.before_bytes > 500
                and curr.request_id != prev.request_id):
            # Check timestamps for rapid-fire
            try:
                t1 = datetime.fromisoformat(prev.ts)
                t2 = datetime.fromisoformat(curr.ts)
                delta = abs((t2 - t1).total_seconds())
                if delta < 3:
                    issues.append(
                        f"Rapid-fire duplicate: turns {prev.index}→{curr.index} "
                        f"({curr.before_bytes:,}B, {delta:.1f}s apart)"
                    )
            except (ValueError, TypeError):
                pass

    # High error rate
    if trace.error_count > len(trace.turns) * 0.3:
        issues.append(f"High error rate: {trace.error_count}/{len(trace.turns)} turns errored ({trace.error_count/len(trace.turns)*100:.0f}%)")

    return issues


# ──────────────────────────────────────────────────────────────────────
# CLI: list subcommand
# ──────────────────────────────────────────────────────────────────────

def cmd_list(args: argparse.Namespace) -> int:
    """List available session traces."""
    files = _list_trace_files(include_audited=args.all)
    if not files:
        print(f"  {C.RED}No session traces found.{C.RESET}")
        return 1

    print(f"  {C.BOLD}Available session traces{C.RESET} ({len(files)} found)\n")
    headers = ["#", "Session ID", "Size", "Lines", "Modified"]
    rows = []
    for i, f in enumerate(files, 1):
        sid = f.stem.removeprefix("iq-session-")
        size = _fmt_bytes(f.stat().st_size)
        lines = sum(1 for _ in f.open("r", encoding="utf-8"))
        mtime = datetime.fromtimestamp(f.stat().st_mtime).strftime("%Y-%m-%d %H:%M")
        audited = " (audited)" if "audited" in str(f) else ""
        rows.append([str(i), sid + audited, size, str(lines), mtime])

    _print_table(headers, rows, col_align=[">", "<", ">", ">", "<"])
    return 0


# ──────────────────────────────────────────────────────────────────────
# CLI: audit subcommand (default)
# ──────────────────────────────────────────────────────────────────────

def cmd_audit(args: argparse.Namespace) -> int:
    """Run the full Bedrock-powered audit pipeline."""
    # Housekeeping
    deleted = auto_rotate_traces()
    if deleted:
        logger.info("Rotated %d old trace files", deleted)

    filepath = _resolve_session_file(args.session)
    if filepath is None:
        print(f"  {C.RED}No session traces found.{C.RESET}")
        print(f"  Run {C.GREEN}iq claude --dev --dump-payloads{C.RESET} first to collect traces.\n")
        return 1

    # Parse
    trace = parse_trace(filepath)
    if not trace.turns:
        print(f"  {C.RED}Session trace is empty or fully corrupted.{C.RESET}")
        return 1

    print(f"  Session:  {C.YELLOW}{trace.session_id}{C.RESET}")
    print(f"  Turns:    {C.GREEN}{len(trace.turns)}{C.RESET}  |  "
          f"Errors: {trace.error_count}  |  Models: {', '.join(trace.models_used)}")
    print(f"  Volume:   {_fmt_bytes(trace.total_before)} total  →  "
          f"{C.GREEN}{_fmt_bytes(trace.total_saved)} saved ({trace.total_reduction_pct:.1f}%){C.RESET}\n")

    # Build prompt & invoke
    prompt = build_audit_prompt(trace)
    prompt_kb = len(prompt) / 1024
    est_tokens = len(prompt) // 4
    print(f"  Prompt:   {prompt_kb:.0f} KB (~{est_tokens:,} tokens est.)")
    print(f"  Invoking Bedrock...\n")

    report = run_bedrock_audit(prompt)
    if report is None:
        return 1

    # Post-audit pipeline
    print(f"\n  {C.BOLD}Post-Audit Pipeline{C.RESET}")
    run_post_audit(trace, report)

    # Print health score if present
    score = report.get("session_health_score")
    if score is not None:
        color = C.GREEN if score >= 70 else (C.YELLOW if score >= 40 else C.RED)
        print(f"\n  {C.BOLD}Session Health Score: {color}{score}/100{C.RESET}\n")

    print(f"  {C.GREEN}✓ Audit complete.{C.RESET}\n")
    return 0


# ──────────────────────────────────────────────────────────────────────
# CLI entry point
# ──────────────────────────────────────────────────────────────────────

def main() -> int:
    parser = argparse.ArgumentParser(
        prog="iq-audit",
        description="IndexQube Session Audit Tool — analyze LLM proxy optimization traces",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=textwrap.dedent("""\
            examples:
              python3 scripts/audit_session.py                  # audit latest trace
              python3 scripts/audit_session.py --session <id>   # audit specific session
              python3 scripts/audit_session.py stats             # local stats (no Bedrock)
              python3 scripts/audit_session.py list              # list available traces
              python3 scripts/audit_session.py list --all        # include audited traces

            environment:
              IQ_BEDROCK_REGION   AWS region for Bedrock (default: us-east-1)
              IQ_S3_BUCKET        S3 bucket for trace archival
              IQ_ENABLE_S3        Set to 0 to disable S3 upload
              IQ_ENABLE_CW        Set to 0 to disable CloudWatch metrics
              NO_COLOR            Disable colored output
        """),
    )
    parser.add_argument(
        "-v", "--verbose", action="store_true",
        help="Enable debug logging",
    )
    parser.add_argument(
        "--session", metavar="ID_OR_PATH",
        help="Session ID or path to a specific trace file",
    )

    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # audit (default)
    p_audit = subparsers.add_parser("audit", help="Run full Bedrock-powered audit (default)")
    p_audit.add_argument("--session", metavar="ID_OR_PATH", help="Session ID or path")

    # stats
    p_stats = subparsers.add_parser("stats", help="Print local stats without calling Bedrock")
    p_stats.add_argument("--session", metavar="ID_OR_PATH", help="Session ID or path")

    # list
    p_list = subparsers.add_parser("list", help="List available session traces")
    p_list.add_argument("--all", action="store_true", help="Include already-audited traces")

    args = parser.parse_args()

    # Logging setup
    level = logging.DEBUG if args.verbose else logging.WARNING
    logging.basicConfig(format=LOG_FORMAT, datefmt=LOG_DATEFMT, level=level, stream=sys.stderr)

    # Banner
    print(f"\n{C.BLUE}{'━' * 64}{C.RESET}")
    print(f"{C.BLUE}  IndexQube Auto-Optimization Audit Tool{C.RESET}")
    print(f"{C.BLUE}{'━' * 64}{C.RESET}\n")

    # Dispatch
    cmd = args.command
    if cmd == "list":
        return cmd_list(args)
    elif cmd == "stats":
        return cmd_stats(args)
    else:
        return cmd_audit(args)


if __name__ == "__main__":
    raise SystemExit(main())
