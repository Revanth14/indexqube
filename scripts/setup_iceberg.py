import argparse
import os
import sys
import time
from pathlib import Path

import boto3
import pyarrow as pa
from dotenv import load_dotenv
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.schema import Schema
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.transforms import MonthTransform, IdentityTransform, YearTransform
from pyiceberg.types import (
    BooleanType,
    DateType,
    DecimalType,
    IntegerType,
    LongType,
    NestedField,
    StringType,
    TimestamptzType,
)
from sqlalchemy import create_engine, text

# Config
load_dotenv(Path(__file__).resolve().parent.parent / ".env")

AWS_REGION   = os.getenv("AWS_REGION", "us-east-1")
ACCOUNT_ID   = boto3.client("sts").get_caller_identity()["Account"]

S3_RAW       = os.getenv("S3_RAW_BUCKET",     f"indexqube-raw-{ACCOUNT_ID}")
S3_CURATED   = os.getenv("S3_CURATED_BUCKET",  f"indexqube-curated-{ACCOUNT_ID}")
S3_GOLD      = os.getenv("S3_GOLD_BUCKET",     f"indexqube-gold-{ACCOUNT_ID}")

WAREHOUSE    = f"s3://{S3_CURATED}/iceberg"     # Iceberg warehouse root
CATALOG_NAME = "indexqube"

DB_URI = (
    f"postgresql://{os.getenv('RDS_USER', 'postgres')}:"
    f"{os.getenv('RDS_PASSWORD', '')}@"
    f"{os.getenv('RDS_HOST', 'localhost')}:"
    f"{os.getenv('RDS_PORT', '5432')}/"
    f"{os.getenv('RDS_DB', 'indexqube')}"
)


# Iceberg Schemas
# Mirror the PostgreSQL tables exactly, with Iceberg-native types.

MARKET_DATA_EOD_SCHEMA = Schema(
    NestedField(field_id=1,  name="ticker",         type=StringType(),       required=True),
    NestedField(field_id=2,  name="date",            type=DateType(),         required=True),
    NestedField(field_id=3,  name="open",            type=DecimalType(12, 4), required=False),
    NestedField(field_id=4,  name="high",            type=DecimalType(12, 4), required=False),
    NestedField(field_id=5,  name="low",             type=DecimalType(12, 4), required=False),
    NestedField(field_id=6,  name="close",           type=DecimalType(12, 4), required=False),
    NestedField(field_id=7,  name="volume",          type=LongType(),         required=False),
    NestedField(field_id=8,  name="adjusted_close",  type=DecimalType(12, 4), required=False),
    NestedField(field_id=9,  name="provider",        type=StringType(),       required=True),
    NestedField(field_id=10, name="created_at",      type=TimestamptzType(),  required=False),
)

MARKET_DATA_EOD_PARTITION = PartitionSpec(
    PartitionField(source_id=2, field_id=1001, transform=MonthTransform(), name="date_month"),
)

INDEX_VALUES_SCHEMA = Schema(
    NestedField(field_id=1, name="index_ticker", type=StringType(),       required=True),
    NestedField(field_id=2, name="date",          type=DateType(),         required=True),
    NestedField(field_id=3, name="value",         type=DecimalType(12, 4), required=True),
    NestedField(field_id=4, name="return_pct",    type=DecimalType(8, 4),  required=False),
    NestedField(field_id=5, name="created_at",    type=TimestamptzType(),  required=False),
)

INDEX_VALUES_PARTITION = PartitionSpec(
    PartitionField(source_id=2, field_id=1001, transform=MonthTransform(), name="date_month"),
)

INDEX_CONSTITUENTS_SCHEMA = Schema(
    NestedField(field_id=1, name="index_ticker",    type=StringType(), required=True),
    NestedField(field_id=2, name="ticker",           type=StringType(), required=True),
    NestedField(field_id=3, name="weight",           type=DecimalType(8, 4), required=False),
    NestedField(field_id=4, name="effective_date",   type=DateType(),        required=True),
    NestedField(field_id=5, name="created_at",       type=TimestamptzType(), required=False),
)

INDEX_CONSTITUENTS_PARTITION = PartitionSpec(
    PartitionField(source_id=4, field_id=1001, transform=MonthTransform(), name="effective_month"),
)

CORPORATE_ACTIONS_SCHEMA = Schema(
    NestedField(field_id=1, name="ticker",       type=StringType(),        required=True),
    NestedField(field_id=2, name="action_type",  type=StringType(),        required=True),
    NestedField(field_id=3, name="ex_date",      type=DateType(),          required=True),
    NestedField(field_id=4, name="payment_date", type=DateType(),          required=False),
    NestedField(field_id=5, name="ratio",        type=DecimalType(10, 6),  required=False),
    NestedField(field_id=6, name="amount",       type=DecimalType(10, 4),  required=False),
    NestedField(field_id=7, name="provider",     type=StringType(),        required=False),
    NestedField(field_id=8, name="created_at",   type=TimestamptzType(),   required=False),
)

CORPORATE_ACTIONS_PARTITION = PartitionSpec(
    PartitionField(source_id=3, field_id=1001, transform=YearTransform(), name="ex_year"),
)

VALIDATION_RESULTS_SCHEMA = Schema(
    NestedField(field_id=1,  name="validation_date", type=DateType(),         required=True),
    NestedField(field_id=2,  name="ticker",           type=StringType(),       required=True),
    NestedField(field_id=3,  name="validation_type",  type=StringType(),       required=True),
    NestedField(field_id=4,  name="severity",         type=StringType(),       required=True),
    NestedField(field_id=5,  name="provider_a",       type=StringType(),       required=False),
    NestedField(field_id=6,  name="provider_b",       type=StringType(),       required=False),
    NestedField(field_id=7,  name="expected_value",   type=DecimalType(12, 4), required=False),
    NestedField(field_id=8,  name="actual_value",     type=DecimalType(12, 4), required=False),
    NestedField(field_id=9,  name="details",          type=StringType(),       required=False),  # JSONB → string
    NestedField(field_id=10, name="resolved",         type=BooleanType(),      required=False),
    NestedField(field_id=11, name="created_at",       type=TimestamptzType(),  required=False),
)

VALIDATION_RESULTS_PARTITION = PartitionSpec(
    PartitionField(source_id=1, field_id=1001, transform=MonthTransform(), name="validation_month"),
)

SP500_CONSTITUENTS_SCHEMA = Schema(
    NestedField(field_id=1, name="ticker",       type=StringType(),  required=True),
    NestedField(field_id=2, name="company_name", type=StringType(),  required=False),
    NestedField(field_id=3, name="sector",       type=StringType(),  required=False),
    NestedField(field_id=4, name="industry",     type=StringType(),  required=False),
    NestedField(field_id=5, name="added_date",   type=DateType(),    required=False),
    NestedField(field_id=6, name="is_active",    type=BooleanType(), required=False),
)

# Table definitions registry
TABLE_DEFS = {
    "market_data_eod": {
        "namespace":      "raw",
        "schema":         MARKET_DATA_EOD_SCHEMA,
        "partition_spec": MARKET_DATA_EOD_PARTITION,
        "pg_query":       "SELECT ticker, date, open, high, low, close, volume, adjusted_close, provider, created_at FROM market_data_eod ORDER BY date",
        "description":    "End-of-day market prices (partitioned by month)",
    },
    "index_values": {
        "namespace":      "gold",
        "schema":         INDEX_VALUES_SCHEMA,
        "partition_spec": INDEX_VALUES_PARTITION,
        "pg_query":       "SELECT index_ticker, date, value, return_pct, created_at FROM index_values ORDER BY date",
        "description":    "Calculated index values (partitioned by month)",
    },
    "index_constituents": {
        "namespace":      "gold",
        "schema":         INDEX_CONSTITUENTS_SCHEMA,
        "partition_spec": INDEX_CONSTITUENTS_PARTITION,
        "pg_query":       "SELECT index_ticker, ticker, weight, effective_date, created_at FROM index_constituents ORDER BY effective_date",
        "description":    "Index composition and weights (partitioned by month)",
    },
    "corporate_actions": {
        "namespace":      "raw",
        "schema":         CORPORATE_ACTIONS_SCHEMA,
        "partition_spec": CORPORATE_ACTIONS_PARTITION,
        "pg_query":       "SELECT ticker, action_type, ex_date, payment_date, ratio, amount, provider, created_at FROM corporate_actions ORDER BY ex_date",
        "description":    "Corporate actions: splits, dividends (partitioned by year)",
    },
    "validation_results": {
        "namespace":      "curated",
        "schema":         VALIDATION_RESULTS_SCHEMA,
        "partition_spec": VALIDATION_RESULTS_PARTITION,
        "pg_query":       "SELECT validation_date, ticker, validation_type, severity, provider_a, provider_b, expected_value, actual_value, details::text, resolved, created_at FROM validation_results ORDER BY validation_date",
        "description":    "Data quality validation results (partitioned by month)",
    },
    "sp500_constituents": {
        "namespace":      "raw",
        "schema":         SP500_CONSTITUENTS_SCHEMA,
        "partition_spec": PartitionSpec(),  # no partition – small reference table
        "pg_query":       "SELECT ticker, company_name, sector, industry, NULL::date AS added_date, is_active FROM sp500_constituents ORDER BY ticker",
        "description":    "S&P 500 constituent reference data",
    },
}


# Helpers
def get_catalog() -> SqlCatalog:
    """
    Create a SQL-backed Iceberg catalog stored in PostgreSQL (same RDS instance).
    The catalog metadata tables live in a dedicated 'iceberg_catalog' schema.
    """
    return SqlCatalog(
        CATALOG_NAME,
        **{
            "uri":              DB_URI,
            "warehouse":        WAREHOUSE,
            "s3.region":        AWS_REGION,
            "s3.endpoint":      f"https://s3.{AWS_REGION}.amazonaws.com",
        },
    )


def get_pg_engine():
    return create_engine(DB_URI)


def log(msg: str):
    print(f"  → {msg}")


# Core logic functions
def create_namespaces(catalog: SqlCatalog, dry_run: bool = False):
    """Create raw / curated / gold namespaces."""
    namespaces = {
        "raw":     {"zone": "raw",     "s3_bucket": S3_RAW,     "description": "Raw ingested data from providers"},
        "curated": {"zone": "curated", "s3_bucket": S3_CURATED, "description": "Validated and cleaned data"},
        "gold":    {"zone": "gold",    "s3_bucket": S3_GOLD,    "description": "Final calculated indices and analytics"},
    }

    for ns, props in namespaces.items():
        existing = [n for n in catalog.list_namespaces() if n == (ns,)]
        if existing:
            log(f"Namespace '{ns}' already exists, skipping")
            continue
        if dry_run:
            log(f"[DRY-RUN] Would create namespace '{ns}' with properties {props}")
        else:
            catalog.create_namespace(ns, properties=props)
            log(f"Created namespace '{ns}'")


def create_tables(catalog: SqlCatalog, dry_run: bool = False):
    """Create Iceberg tables mirroring the Postgres schema."""
    for table_name, defn in TABLE_DEFS.items():
        ns = defn["namespace"]
        full_name = f"{ns}.{table_name}"

        try:
            catalog.load_table(full_name)
            log(f"Table '{full_name}' already exists, skipping")
            continue
        except Exception:
            pass  # table doesn't exist yet - create it

        if dry_run:
            log(f"[DRY-RUN] Would create table '{full_name}' – {defn['description']}")
            log(f"           Schema fields: {[f.name for f in defn['schema'].fields]}")
            log(f"           Partition spec: {defn['partition_spec']}")
            continue

        catalog.create_table(
            identifier=full_name,
            schema=defn["schema"],
            partition_spec=defn["partition_spec"],
            properties={
                "description":        defn["description"],
                "write.format.default": "parquet",
                "write.parquet.compression-codec": "zstd",
            },
        )
        log(f"Created table '{full_name}'")


def _cast_arrow_to_iceberg_schema(arrow_table: pa.Table, iceberg_table) -> pa.Table:
    """
    Cast a PyArrow table so its schema exactly matches the Iceberg table's
    Arrow schema (types, nullability, field names).
    Handles: float→decimal, string→date, nullable→required, etc.
    """
    import decimal as _decimal

    target_schema = iceberg_table.schema().as_arrow()
    new_columns = []

    for target_field in target_schema:
        src_col = arrow_table.column(target_field.name)
        src_type = src_col.type
        tgt_type = target_field.type

        # float / double → decimal
        if pa.types.is_floating(src_type) and pa.types.is_decimal(tgt_type):
            precision, scale = tgt_type.precision, tgt_type.scale
            py_values = src_col.to_pylist()
            converted = [
                _decimal.Decimal(str(round(v, scale))) if v is not None else None
                for v in py_values
            ]
            src_col = pa.array(converted, type=tgt_type)

        # string / object → date (Pandas sometimes reads date columns as strings)
        elif pa.types.is_string(src_type) and pa.types.is_date(tgt_type):
            from datetime import date as _date
            py_values = src_col.to_pylist()
            converted = [
                _date.fromisoformat(v) if v is not None and v != "None" else None
                for v in py_values
            ]
            src_col = pa.array(converted, type=tgt_type)

        # timestamp string → date
        elif pa.types.is_timestamp(src_type) and pa.types.is_date(tgt_type):
            src_col = src_col.cast(tgt_type)

        # Same logical type but different nullability / metadata → cast
        elif src_type != tgt_type:
            try:
                src_col = src_col.cast(tgt_type)
            except (pa.ArrowInvalid, pa.ArrowNotImplementedError):
                pass  # keep as-is; Iceberg will raise a clear error

        new_columns.append(src_col)

    return pa.table(
        {field.name: col for field, col in zip(target_schema, new_columns)},
        schema=target_schema,
    )


def migrate_table(catalog: SqlCatalog, table_name: str, batch_size: int = 50_000, dry_run: bool = False):
    """
    Migrate one Postgres table → Iceberg table via PyArrow.
    Reads in batches to avoid memory issues on large tables.
    """
    import pandas as pd

    defn = TABLE_DEFS[table_name]
    full_name = f"{defn['namespace']}.{table_name}"

    engine = get_pg_engine()
    with engine.connect() as conn:
        count = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}")).scalar()

    if count == 0:
        log(f"Table '{table_name}' is empty in Postgres, nothing to migrate")
        return

    if dry_run:
        log(f"[DRY-RUN] Would migrate {count:,} rows from Postgres '{table_name}' → Iceberg '{full_name}'")
        return

    log(f"Migrating {count:,} rows: Postgres '{table_name}' → Iceberg '{full_name}'")

    iceberg_table = catalog.load_table(full_name)
    total_written = 0
    start = time.time()

    for offset in range(0, count, batch_size):
        query = f"{defn['pg_query']} OFFSET {offset} LIMIT {batch_size}"

        df = pd.read_sql(text(query), engine)
        if df.empty:
            break

        # Convert JSONB / object columns to string for Iceberg compatibility
        for col in df.columns:
            if df[col].dtype == object and col not in ("ticker", "provider", "action_type",
                "index_ticker", "validation_type", "severity", "provider_a", "provider_b",
                "company_name", "sector", "industry"):
                df[col] = df[col].astype(str)

        arrow_table = pa.Table.from_pandas(df, preserve_index=False)

        # Cast to match Iceberg schema exactly (types + nullability)
        arrow_table = _cast_arrow_to_iceberg_schema(arrow_table, iceberg_table)

        iceberg_table.append(arrow_table)
        total_written += len(df)

        elapsed = time.time() - start
        rate = total_written / elapsed if elapsed > 0 else 0
        log(f"  {total_written:,}/{count:,} rows ({rate:,.0f} rows/s)")

    elapsed = time.time() - start
    log(f"✓ Migrated {total_written:,} rows in {elapsed:.1f}s")


def migrate_all(catalog: SqlCatalog, dry_run: bool = False):
    """Migrate all tables from Postgres to Iceberg."""
    for table_name in TABLE_DEFS:
        migrate_table(catalog, table_name, dry_run=dry_run)


def verify_tables(catalog: SqlCatalog):
    """Print row counts and metadata for all Iceberg tables."""
    print("\n─── Iceberg Table Verification ───")
    for table_name, defn in TABLE_DEFS.items():
        full_name = f"{defn['namespace']}.{table_name}"
        try:
            tbl = catalog.load_table(full_name)
            scan = tbl.scan()
            row_count = scan.to_arrow().num_rows
            snapshots = tbl.metadata.snapshots
            print(f"  {full_name:40s}  rows: {row_count:>10,}  snapshots: {len(snapshots)}")
        except Exception as e:
            print(f"  {full_name:40s}  ERROR: {e}")
    print()


# cli
def main():
    parser = argparse.ArgumentParser(
        description="IndexQube – Iceberg catalog & table setup",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("--migrate",  action="store_true", help="Back-fill data from Postgres into Iceberg tables")
    parser.add_argument("--verify",   action="store_true", help="Print row counts for all Iceberg tables")
    parser.add_argument("--dry-run",  action="store_true", help="Show what would happen without making changes")
    parser.add_argument("--table",    type=str, default=None, help="Migrate a single table (e.g. market_data_eod)")
    args = parser.parse_args()

    print("=" * 60)
    print("  IndexQube – Apache Iceberg Setup")
    print("=" * 60)
    print(f"  Catalog:   {CATALOG_NAME}")
    print(f"  Warehouse: {WAREHOUSE}")
    print(f"  Region:    {AWS_REGION}")
    print(f"  Postgres:  {os.getenv('RDS_HOST', 'localhost')}:{os.getenv('RDS_PORT', '5432')}/{os.getenv('RDS_DB', 'indexqube')}")
    print(f"  S3 Raw:    {S3_RAW}")
    print(f"  S3 Curated:{S3_CURATED}")
    print(f"  S3 Gold:   {S3_GOLD}")
    if args.dry_run:
        print("  Mode:      DRY-RUN (no changes)")
    print("=" * 60)
    print()

    # 1. Create catalog
    print("[1/4] Initializing Iceberg catalog …")
    catalog = get_catalog()
    log("SQL catalog connected")
    print()

    # 2. Create namespaces
    print("[2/4] Creating namespaces (raw / curated / gold) …")
    create_namespaces(catalog, dry_run=args.dry_run)
    print()

    # 3. Create tables
    print("[3/4] Creating Iceberg tables …")
    create_tables(catalog, dry_run=args.dry_run)
    print()

    # 4. Migrate / verify
    if args.migrate:
        print("[4/4] Migrating Postgres → Iceberg …")
        if args.table:
            if args.table not in TABLE_DEFS:
                print(f"  ERROR: Unknown table '{args.table}'. Available: {list(TABLE_DEFS.keys())}")
                sys.exit(1)
            migrate_table(catalog, args.table, dry_run=args.dry_run)
        else:
            migrate_all(catalog, dry_run=args.dry_run)
    elif args.verify:
        print("[4/4] Verifying Iceberg tables …")
        verify_tables(catalog)
    else:
        print("[4/4] Skipping migration (use --migrate to back-fill data)")
    print()

    print("Done! ✓")


if __name__ == "__main__":
    main()
