import argparse
import os
from datetime import datetime
from pathlib import Path

import boto3
import pandas as pd
import pyarrow as pa
from dotenv import load_dotenv
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.expressions import EqualTo, GreaterThanOrEqual, LessThanOrEqual, And

# Config
load_dotenv(Path(__file__).resolve().parent.parent / ".env")

AWS_REGION   = os.getenv("AWS_REGION", "us-east-1")
ACCOUNT_ID   = boto3.client("sts").get_caller_identity()["Account"]
S3_CURATED   = os.getenv("S3_CURATED_BUCKET", f"indexqube-curated-{ACCOUNT_ID}")
WAREHOUSE    = f"s3://{S3_CURATED}/iceberg"
CATALOG_NAME = "indexqube"

DB_URI = (
    f"postgresql://{os.getenv('RDS_USER', 'postgres')}:"
    f"{os.getenv('RDS_PASSWORD', '')}@"
    f"{os.getenv('RDS_HOST', 'localhost')}:"
    f"{os.getenv('RDS_PORT', '5432')}/"
    f"{os.getenv('RDS_DB', 'indexqube')}"
)

ALL_TABLES = [
    "raw.market_data_eod",
    "gold.index_values",
    "gold.index_constituents",
    "raw.corporate_actions",
    "curated.validation_results",
    "raw.sp500_constituents",
]


def get_catalog() -> SqlCatalog:
    return SqlCatalog(
        CATALOG_NAME,
        **{
            "uri":         DB_URI,
            "warehouse":   WAREHOUSE,
            "s3.region":   AWS_REGION,
            "s3.endpoint": f"https://s3.{AWS_REGION}.amazonaws.com",
        },
    )


def fmt_ts(ts_ms: int | None) -> str:
    """Format a millisecond timestamp to a human-readable string."""
    if ts_ms is None:
        return "N/A"
    return datetime.fromtimestamp(ts_ms / 1000).strftime("%Y-%m-%d %H:%M:%S")


def fmt_bytes(size: int) -> str:
    """Format bytes to human-readable size."""
    for unit in ("B", "KB", "MB", "GB"):
        if size < 1024:
            return f"{size:.1f} {unit}"
        size /= 1024
    return f"{size:.1f} TB"


# Command: history
def cmd_history(args):
    """Show snapshot history for all (or one) Iceberg tables."""
    catalog = get_catalog()
    tables = [args.table] if args.table else ALL_TABLES

    for table_name in tables:
        try:
            tbl = catalog.load_table(table_name)
        except Exception as e:
            print(f"\n⚠  Table '{table_name}' not found: {e}")
            continue

        snapshots = tbl.metadata.snapshots
        current_id = tbl.metadata.current_snapshot_id

        print(f"\n{'═' * 80}")
        print(f"  {table_name}  –  {len(snapshots)} snapshot(s)")
        print(f"{'═' * 80}")

        if not snapshots:
            print("  (no snapshots yet)\n")
            continue

        rows = []
        for snap in snapshots:
            summary = snap.summary or {}
            is_current = "→" if snap.snapshot_id == current_id else " "
            rows.append({
                "": is_current,
                "Snapshot ID": snap.snapshot_id,
                "Timestamp": fmt_ts(snap.timestamp_ms),
                "Operation": summary.get("operation", "?"),
                "Added Rows": summary.get("added-data-files", "-"),
                "Total Records": summary.get("total-records", "-"),
                "Total Size": fmt_bytes(int(summary.get("total-files-size", 0))),
            })

        df = pd.DataFrame(rows)
        print(df.to_string(index=False))
        print(f"\n  → = current snapshot")
    print()


# Command: query
def cmd_query(args):
    """Query a table at a specific snapshot or timestamp."""
    catalog = get_catalog()
    tbl = catalog.load_table(args.table)

    if args.snapshot_id:
        # Time travel to a specific snapshot
        snapshot_id = int(args.snapshot_id)
        print(f"\n⏱  Time-traveling '{args.table}' to snapshot {snapshot_id}")
        arrow_table = tbl.scan(snapshot_id=snapshot_id).to_arrow()

    elif args.as_of:
        # Time travel to a specific timestamp
        ts = datetime.fromisoformat(args.as_of)
        ts_ms = int(ts.timestamp() * 1000)
        print(f"\n⏱  Time-traveling '{args.table}' to {args.as_of}")

        # Find the snapshot that was current at that timestamp
        target_snap = None
        for snap in sorted(tbl.metadata.snapshots, key=lambda s: s.timestamp_ms):
            if snap.timestamp_ms <= ts_ms:
                target_snap = snap
            else:
                break

        if target_snap is None:
            print(f"  ⚠  No snapshot exists before {args.as_of}")
            return

        print(f"  Using snapshot {target_snap.snapshot_id} ({fmt_ts(target_snap.timestamp_ms)})")
        arrow_table = tbl.scan(snapshot_id=target_snap.snapshot_id).to_arrow()

    else:
        # Current state
        print(f"\n📋  Current state of '{args.table}'")
        arrow_table = tbl.scan().to_arrow()

    df = arrow_table.to_pandas()
    total = len(df)
    limit = args.limit or 20

    print(f"  Total rows: {total:,}")
    print(f"  Showing first {min(limit, total)} rows:\n")
    print(df.head(limit).to_string(index=False))
    print()

# Command: diff
def cmd_diff(args):
    """Compare two snapshots of a table — show what changed."""
    catalog = get_catalog()
    tbl = catalog.load_table(args.table)

    snapshots = tbl.metadata.snapshots
    if len(snapshots) < 2:
        print(f"\n⚠  '{args.table}' has only {len(snapshots)} snapshot(s), need at least 2 for diff")
        return

    # Use provided snapshot IDs or default to last two
    if args.old and args.new:
        old_id, new_id = int(args.old), int(args.new)
    else:
        sorted_snaps = sorted(snapshots, key=lambda s: s.timestamp_ms)
        old_id = sorted_snaps[-2].snapshot_id
        new_id = sorted_snaps[-1].snapshot_id

    old_snap = next((s for s in snapshots if s.snapshot_id == old_id), None)
    new_snap = next((s for s in snapshots if s.snapshot_id == new_id), None)

    if not old_snap or not new_snap:
        print(f"  ⚠  Snapshot not found")
        return

    print(f"\n{'═' * 80}")
    print(f"  DIFF: {args.table}")
    print(f"{'═' * 80}")
    print(f"  OLD: snapshot {old_id}  ({fmt_ts(old_snap.timestamp_ms)})")
    print(f"  NEW: snapshot {new_id}  ({fmt_ts(new_snap.timestamp_ms)})")
    print(f"{'─' * 80}")

    # Summary comparison
    old_summary = old_snap.summary or {}
    new_summary = new_snap.summary or {}

    metrics = [
        ("total-records", "Total Records"),
        ("total-data-files", "Data Files"),
        ("total-files-size", "Total Size (bytes)"),
        ("added-records", "Added Records"),
        ("deleted-records", "Deleted Records"),
        ("added-data-files", "Added Files"),
        ("deleted-data-files", "Deleted Files"),
    ]

    print(f"\n  {'Metric':<25s}  {'Old':>15s}  {'New':>15s}  {'Delta':>15s}")
    print(f"  {'─' * 25}  {'─' * 15}  {'─' * 15}  {'─' * 15}")
    for key, label in metrics:
        old_val = int(old_summary.get(key) or 0)
        new_val = int(new_summary.get(key) or 0)
        delta = new_val - old_val
        delta_str = f"+{delta}" if delta > 0 else str(delta)
        if key == "total-files-size":
            print(f"  {label:<25s}  {fmt_bytes(old_val):>15s}  {fmt_bytes(new_val):>15s}  {delta_str:>15s}")
        else:
            print(f"  {label:<25s}  {old_val:>15,}  {new_val:>15,}  {delta_str:>15s}")

    # Data-level diff: load both snapshots and compare
    if args.data:
        print(f"\n  Loading data from both snapshots for row-level diff...")

        old_df = tbl.scan(snapshot_id=old_id).to_arrow().to_pandas()
        new_df = tbl.scan(snapshot_id=new_id).to_arrow().to_pandas()

        # Find key columns for comparison
        key_cols = []
        all_cols = list(new_df.columns)
        for candidate in ["ticker", "date", "index_ticker", "validation_date", "ex_date"]:
            if candidate in all_cols:
                key_cols.append(candidate)

        if key_cols:
            old_keys = set(old_df[key_cols].apply(tuple, axis=1))
            new_keys = set(new_df[key_cols].apply(tuple, axis=1))

            added = new_keys - old_keys
            removed = old_keys - new_keys

            print(f"\n  Key columns: {key_cols}")
            print(f"  Rows added:   {len(added):,}")
            print(f"  Rows removed: {len(removed):,}")

            if added and len(added) <= 20:
                print(f"\n  Added rows (sample):")
                for row in list(added)[:20]:
                    print(f"    {row}")

            if removed and len(removed) <= 20:
                print(f"\n  Removed rows (sample):")
                for row in list(removed)[:20]:
                    print(f"    {row}")
        else:
            print(f"\n  Old snapshot rows: {len(old_df):,}")
            print(f"  New snapshot rows: {len(new_df):,}")

    print()

# Command: audit
def cmd_audit(args):
    """Show how a specific ticker's data changed across snapshots."""
    catalog = get_catalog()
    ticker = args.ticker.upper()
    table_name = args.table or "raw.market_data_eod"

    tbl = catalog.load_table(table_name)
    snapshots = sorted(tbl.metadata.snapshots, key=lambda s: s.timestamp_ms)

    print(f"\n{'═' * 80}")
    print(f"  AUDIT TRAIL: {ticker} in {table_name}")
    print(f"  {len(snapshots)} snapshot(s)")
    print(f"{'═' * 80}")

    prev_count = None
    prev_latest_date = None

    for snap in snapshots:
        try:
            scan = tbl.scan(snapshot_id=snap.snapshot_id)

            # Filter for this ticker
            if "ticker" in [f.name for f in tbl.schema().fields]:
                scan = scan.filter(EqualTo("ticker", ticker))
            elif "index_ticker" in [f.name for f in tbl.schema().fields]:
                scan = scan.filter(EqualTo("index_ticker", ticker))
            else:
                print(f"  ⚠  No ticker column found in {table_name}")
                return

            arrow = scan.to_arrow()
            df = arrow.to_pandas()
            count = len(df)

            # Find latest date in this snapshot
            date_col = "date" if "date" in df.columns else "ex_date" if "ex_date" in df.columns else None
            latest_date = str(df[date_col].max()) if date_col and not df.empty else "N/A"

            # Detect changes
            change = ""
            if prev_count is not None:
                delta = count - prev_count
                if delta > 0:
                    change = f"  (+{delta} rows)"
                elif delta < 0:
                    change = f"  ({delta} rows)"
                elif latest_date != prev_latest_date:
                    change = f"  (data updated)"

            ts_str = fmt_ts(snap.timestamp_ms)
            op = (snap.summary or {}).get("operation", "?")
            current = " ← current" if snap.snapshot_id == tbl.metadata.current_snapshot_id else ""

            print(f"\n  Snapshot {snap.snapshot_id} | {ts_str} | {op}")
            print(f"    Rows: {count:,}  |  Latest: {latest_date}{change}{current}")

            if not df.empty and date_col and args.detail:
                print(f"    Date range: {df[date_col].min()} → {df[date_col].max()}")
                if "close" in df.columns:
                    latest_row = df.sort_values(date_col).iloc[-1]
                    print(f"    Latest close: ${float(latest_row['close']):,.2f}")

            prev_count = count
            prev_latest_date = latest_date

        except Exception as e:
            print(f"\n  Snapshot {snap.snapshot_id} | {fmt_ts(snap.timestamp_ms)} | ERROR: {e}")

    print()


# Command: rollback
def cmd_rollback(args):
    """Rollback a table to a previous snapshot."""
    catalog = get_catalog()
    tbl = catalog.load_table(args.table)

    snapshot_id = int(args.snapshot_id)
    target_snap = next(
        (s for s in tbl.metadata.snapshots if s.snapshot_id == snapshot_id), None
    )

    if not target_snap:
        print(f"\n⚠  Snapshot {snapshot_id} not found in '{args.table}'")
        print(f"   Available snapshots:")
        for s in tbl.metadata.snapshots:
            print(f"     {s.snapshot_id}  ({fmt_ts(s.timestamp_ms)})")
        return

    current_snap = next(
        (s for s in tbl.metadata.snapshots if s.snapshot_id == tbl.metadata.current_snapshot_id),
        None,
    )

    print(f"\n{'═' * 80}")
    print(f"  ⚠  ROLLBACK: {args.table}")
    print(f"{'═' * 80}")
    print(f"  Current: snapshot {tbl.metadata.current_snapshot_id} ({fmt_ts(current_snap.timestamp_ms if current_snap else None)})")
    print(f"  Target:  snapshot {snapshot_id} ({fmt_ts(target_snap.timestamp_ms)})")

    current_records = int((current_snap.summary or {}).get("total-records", 0)) if current_snap else 0
    target_records = int((target_snap.summary or {}).get("total-records", 0))
    print(f"  Records: {current_records:,} → {target_records:,}")

    if not args.force:
        print(f"\n  ⚠  This is a destructive operation!")
        print(f"  Re-run with --force to confirm:")
        print(f"  python scripts/iceberg_time_travel.py rollback {args.table} --snapshot-id {snapshot_id} --force")
        return

    # Perform the rollback using manage_snapshots
    with tbl.manage_snapshots() as ms:
        ms.set_current_snapshot(snapshot_id)

    # Verify
    tbl = catalog.load_table(args.table)
    print(f"\n  ✓ Rolled back to snapshot {snapshot_id}")
    print(f"  Current snapshot is now: {tbl.metadata.current_snapshot_id}")
    print()


# Main
def main():
    parser = argparse.ArgumentParser(
        description="IndexQube – Iceberg Time Travel",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    # history
    p_hist = subparsers.add_parser("history", help="Show snapshot history")
    p_hist.add_argument("--table", type=str, default=None, help="Specific table (default: all)")
    p_hist.set_defaults(func=cmd_history)

    # query
    p_query = subparsers.add_parser("query", help="Query table at a point in time")
    p_query.add_argument("table", type=str, help="Table name (e.g. raw.market_data_eod)")
    p_query.add_argument("--snapshot-id", type=str, default=None, help="Snapshot ID to query")
    p_query.add_argument("--as-of", type=str, default=None, help="Timestamp to query (ISO format)")
    p_query.add_argument("--limit", type=int, default=20, help="Max rows to display (default: 20)")
    p_query.set_defaults(func=cmd_query)

    # diff
    p_diff = subparsers.add_parser("diff", help="Compare two snapshots")
    p_diff.add_argument("table", type=str, help="Table name")
    p_diff.add_argument("--old", type=str, default=None, help="Old snapshot ID (default: second-to-last)")
    p_diff.add_argument("--new", type=str, default=None, help="New snapshot ID (default: latest)")
    p_diff.add_argument("--data", action="store_true", help="Include row-level diff (loads both snapshots)")
    p_diff.set_defaults(func=cmd_diff)

    # audit
    p_audit = subparsers.add_parser("audit", help="Audit trail for a ticker across snapshots")
    p_audit.add_argument("ticker", type=str, help="Ticker symbol (e.g. AAPL)")
    p_audit.add_argument("--table", type=str, default="raw.market_data_eod", help="Table to audit")
    p_audit.add_argument("--detail", action="store_true", help="Show detailed metrics per snapshot")
    p_audit.set_defaults(func=cmd_audit)

    # rollback
    p_roll = subparsers.add_parser("rollback", help="Rollback table to a previous snapshot")
    p_roll.add_argument("table", type=str, help="Table name")
    p_roll.add_argument("--snapshot-id", type=str, required=True, help="Snapshot ID to roll back to")
    p_roll.add_argument("--force", action="store_true", help="Confirm destructive rollback")
    p_roll.set_defaults(func=cmd_rollback)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
