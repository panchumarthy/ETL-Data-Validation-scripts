"""
Developer: Satish Panchumarthy

Compare PurchaseOrderDetail_with_header.csv against dbo.PurchaseOrderDetail in SQL Server.
Results are written to a timestamped log file.

Composite key: (PurchaseOrderID, LineNumber) — columns 0 and 1 in the CSV.

CSV column order (with header row):
  PurchaseOrderID, LineNumber, ProductID, UnitPrice, OrderQty, ReceivedQty, RejectedQty, DueDate
"""

import csv
import datetime
import decimal
import logging
import pyodbc
from pathlib import Path

# ---------------------------------------------------------------------------
# Connection — matches LoginInfo.txt / SQL_Server_Connection_Details.txt
# ---------------------------------------------------------------------------
DSN        = "TEST_SQL"
UID        = "sa"
PWD        = ""          # empty password as in LoginInfo.txt
TABLE      = "dbo.PurchaseOrderDetail"
CSV_FILE   = Path(__file__).parent / "PurchaseOrderDetail_with_header.csv"
LOG_DIR    = Path(__file__).parent / "output" / "compare"

# Column names must match the DB column order returned by SELECT *
COLUMNS = [
    "PurchaseOrderID",
    "LineNumber",
    "ProductID",
    "UnitPrice",
    "OrderQty",
    "ReceivedQty",
    "RejectedQty",
    "DueDate",
]

# Composite key columns (index positions in COLUMNS)
KEY_COLS = (0, 1)  # PurchaseOrderID, LineNumber

# Number of rows to skip at the start (0 = no header) and end (0 = no trailer)
HEADER_ROWS  = 1   # set to 0 if the CSV has no header
TRAILER_ROWS = 1   # set to 1+ if the CSV has trailing summary/control rows


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
def setup_logging(log_path: Path) -> logging.Logger:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger("compare_pod")
    logger.setLevel(logging.DEBUG)
    fh = logging.FileHandler(log_path, encoding="utf-8")
    fh.setFormatter(logging.Formatter("%(asctime)s  %(message)s"))
    ch = logging.StreamHandler()
    ch.setFormatter(logging.Formatter("%(message)s"))
    logger.addHandler(fh)
    logger.addHandler(ch)
    return logger


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def normalize(value) -> str:
    """Convert any DB or CSV value to a comparable string.

    Decimals from both the DB (decimal.Decimal) and the CSV (plain strings
    like '149.50') are normalized the same way so trailing zeros never cause
    false mismatches: 149.5000 == 149.50 == 149.5 all become '149.5'.
    """
    if value is None:
        return ""
    if isinstance(value, decimal.Decimal):
        return str(value.normalize())
    if isinstance(value, datetime.datetime):
        return value.strftime("%Y-%m-%d %H:%M:%S.000")
    # For string values coming from the CSV, try to normalize numeric strings
    # the same way DB Decimal values are normalized.
    s = str(value).strip()
    try:
        return str(decimal.Decimal(s).normalize())
    except decimal.InvalidOperation:
        return s


def row_key(row: list | tuple, key_cols: tuple) -> tuple:
    return tuple(normalize(row[i]) for i in key_cols)


# ---------------------------------------------------------------------------
# Load CSV
# ---------------------------------------------------------------------------
def load_csv(path: Path, logger: logging.Logger) -> dict:
    """Return {key: [col_values_as_str]} from the CSV file.

    Skips the first HEADER_ROWS rows and the last TRAILER_ROWS rows so the
    caller never has to worry about whether the file has a header or trailer.
    """
    data: dict[tuple, list[str]] = {}
    with path.open(newline="", encoding="utf-8-sig") as f:
        all_rows = list(csv.reader(f))

    # Trim header and trailer rows
    total = len(all_rows)
    start = HEADER_ROWS
    end   = total - TRAILER_ROWS if TRAILER_ROWS > 0 else total
    data_rows = all_rows[start:end]

    logger.info(f"[CSV] Total rows in file: {total}  |  header={HEADER_ROWS}  trailer={TRAILER_ROWS}  data rows={len(data_rows)}")

    for offset, row in enumerate(data_rows):
        line_no = start + offset + 1   # 1-based line number in the original file
        if not any(row):               # skip blank lines
            continue
        if len(row) != len(COLUMNS):
            logger.warning(f"[CSV] Line {line_no}: expected {len(COLUMNS)} columns, got {len(row)} — skipping")
            continue
        normalized = [normalize(cell) for cell in row]
        key = tuple(normalized[i] for i in KEY_COLS)
        if key in data:
            logger.warning(f"[CSV] Duplicate key {key} at line {line_no} — overwriting")
        data[key] = normalized
    return data


# ---------------------------------------------------------------------------
# Load DB
# ---------------------------------------------------------------------------
def load_db(logger: logging.Logger) -> tuple[dict, list[str]]:
    """Return ({key: [col_values_as_str]}, [column_names]) from the DB table."""
    conn_str = f"DSN={DSN};UID={UID};PWD={PWD};"
    logger.info(f"[DB]  Connecting via DSN={DSN}")
    data: dict[tuple, list[str]] = {}
    with pyodbc.connect(conn_str) as conn:
        cursor = conn.cursor()
        sql = f"SELECT * FROM {TABLE}"
        logger.info(f"[DB]  Executing: {sql}")
        cursor.execute(sql)
        db_columns = [desc[0] for desc in cursor.description]
        logger.info(f"[DB]  Columns returned: {db_columns}")

        for db_row in cursor.fetchall():
            normalized = [normalize(v) for v in db_row]
            key = tuple(normalized[i] for i in KEY_COLS)
            if key in data:
                logger.warning(f"[DB]  Duplicate key {key} — overwriting")
            data[key] = normalized

    return data, db_columns


# ---------------------------------------------------------------------------
# Compare
# ---------------------------------------------------------------------------
def compare(
    csv_data: dict,
    db_data: dict,
    db_columns: list[str],
    logger: logging.Logger,
) -> None:
    all_keys = sorted(set(csv_data) | set(db_data))

    match_count   = 0
    diff_count    = 0
    only_csv      = 0
    only_db       = 0

    logger.info("")
    logger.info("=" * 70)
    logger.info("COMPARISON RESULTS")
    logger.info("=" * 70)

    for key in all_keys:
        in_csv = key in csv_data
        in_db  = key in db_data

        if in_csv and not in_db:
            only_csv += 1
            logger.warning(f"[MISSING-IN-DB]   key={key}")
            continue

        if in_db and not in_csv:
            only_db += 1
            logger.warning(f"[MISSING-IN-CSV]  key={key}")
            continue

        # Both present — compare column by column
        csv_row = csv_data[key]
        db_row  = db_data[key]

        # Align by COLUMNS order (CSV) vs db_columns order
        diffs: list[str] = []
        for col_idx, col_name in enumerate(COLUMNS):
            csv_val = csv_row[col_idx]

            # Find matching DB column (case-insensitive)
            db_col_idx = next(
                (i for i, c in enumerate(db_columns) if c.lower() == col_name.lower()),
                None,
            )
            if db_col_idx is None:
                logger.warning(f"[SCHEMA] Column '{col_name}' not found in DB result for key={key}")
                continue

            db_val = db_row[db_col_idx]

            if csv_val != db_val:
                diffs.append(f"  {col_name}: CSV={csv_val!r}  DB={db_val!r}")

        if diffs:
            diff_count += 1
            logger.error(f"[MISMATCH]  key={key}")
            for d in diffs:
                logger.error(d)
        else:
            match_count += 1
            logger.info(f"[MATCH]  key={key}")

    logger.info("")
    logger.info("=" * 70)
    logger.info("SUMMARY")
    logger.info(f"  Total keys checked : {len(all_keys)}")
    logger.info(f"  Matched            : {match_count}")
    logger.info(f"  Mismatched         : {diff_count}")
    logger.info(f"  Only in CSV        : {only_csv}")
    logger.info(f"  Only in DB         : {only_db}")
    logger.info("=" * 70)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> None:
    dt_fmt   = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
    log_path = LOG_DIR / f"compare_PurchaseOrderDetail_{dt_fmt}.log"
    logger   = setup_logging(log_path)

    logger.info(f"Log file : {log_path}")
    logger.info(f"CSV file : {CSV_FILE}")
    logger.info(f"DB table : {TABLE}")
    logger.info(f"Key cols : {[COLUMNS[i] for i in KEY_COLS]}")
    logger.info("")

    logger.info("[CSV] Loading...")
    csv_data = load_csv(CSV_FILE, logger)
    logger.info(f"[CSV] Loaded {len(csv_data)} rows")

    logger.info("[DB]  Loading...")
    db_data, db_columns = load_db(logger)
    logger.info(f"[DB]  Loaded {len(db_data)} rows")

    compare(csv_data, db_data, db_columns, logger)

    print(f"\nLog written to: {log_path}")


if __name__ == "__main__":
    main()
