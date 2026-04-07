import re
import sys
import pyodbc
import datetime
import csv
import logging
from pathlib import Path
from dataclasses import dataclass, field
from concurrent.futures import ThreadPoolExecutor, as_completed


# ---------------------------------------------------------------------------
# Config dataclass replaces all module-level globals
# ---------------------------------------------------------------------------

@dataclass
class AppConfig:
    config_file: str = ""
    env: str = ""
    app_id: str = ""
    src_dsn: str = ""
    syb_port: str = ""
    tgt_dsn: str = ""
    mssql_port: str = ""
    db: str = ""
    src_user: str = ""
    src_pwd: str = ""
    tgt_user: str = ""
    tgt_pwd: str = ""
    n_sql: int = 0
    base_path: str = ""
    tbl_start: str = ""
    f_path: str = ""
    effective_date: str = ""
    src_dsn_string: str = ""
    tgt_dsn_string: str = ""
    sql: dict = field(default_factory=dict)
    key_sql: list = field(default_factory=list)
    syb_sql: list = field(default_factory=list)
    tblname: dict = field(default_factory=dict)
    tables: list = field(default_factory=list)


# ---------------------------------------------------------------------------
# Logging setup
# ---------------------------------------------------------------------------

def setup_logging(log_path: Path) -> logging.Logger:
    logger = logging.getLogger("ISPcmp")
    logger.setLevel(logging.DEBUG)
    fh = logging.FileHandler(log_path, encoding="utf-8")
    fh.setFormatter(logging.Formatter("%(asctime)s: %(message)s"))
    logger.addHandler(fh)
    return logger


# ---------------------------------------------------------------------------
# Config reader
# ---------------------------------------------------------------------------

def detect_app_env(config_file: str) -> tuple[str, str]:
    """Read the first APPID and ENV values found in the config file."""
    app_id = ""
    env = ""
    with Path(config_file).open("r") as fh:
        for line in fh:
            if ":" not in line:
                continue
            key, val = line.split(":", 1)[0].rstrip(), line.split(":", 1)[1].rstrip()
            if key == "APPID" and not app_id:
                app_id = val
            elif key == "ENV" and not env:
                env = val
            if app_id and env:
                break
    return app_id, env


def read_config(cfg: AppConfig) -> bool:
    """Returns True if the config file was found and parsed, False otherwise."""
    cfg.n_sql = 0
    app_found = False
    read_content = False

    config_path = Path(cfg.config_file)
    if not config_path.exists():
        print("Invalid Config File. Please check Path/Filename")
        return False

    with config_path.open("r") as fh:
        for line in fh:
            if ":" not in line:
                continue
            parts = line.split(":")
            key = parts[0].rstrip()
            val = parts[1].rstrip()

            if val == cfg.app_id:
                app_found = True
            if val == cfg.env and app_found:
                read_content = True

            if read_content:
                if line.rstrip() == "PARM:END":
                    break
                print(f"{key} = {val}")
                match key:
                    case "SRCDSN":
                        cfg.src_dsn = val
                    case "TGTDSN":
                        cfg.tgt_dsn = val
                    case "SRCUSER":
                        cfg.src_user = val
                    case "SRCPASSWORD":
                        cfg.src_pwd = val
                    case "TGTUSER":
                        cfg.tgt_user = val
                    case "TGTPASSWORD":
                        cfg.tgt_pwd = val
                    case "APPPATH":
                        cfg.base_path = (
                            f"{parts[1].rstrip()}:{parts[2].rstrip()}"
                            if len(parts) == 3
                            else parts[1].rstrip()
                        )
                    case "SQL":
                        cfg.sql[cfg.n_sql] = val
                        cfg.n_sql += 1

    cfg.src_dsn_string = (
        f"DSN={cfg.src_dsn};UID={cfg.src_user};PWD={cfg.src_pwd};"
        "textSize=2147483647;LoginTimeOut=0;"
    )
    cfg.tgt_dsn_string = (
        f"DSN={cfg.tgt_dsn};UID={cfg.tgt_user};PWD={cfg.tgt_pwd};"
    )
    return True


# ---------------------------------------------------------------------------
# Directory creation
# ---------------------------------------------------------------------------

def create_dir(cfg: AppConfig) -> str:
    dr = Path(__file__).parent / "output" / cfg.app_id / cfg.env
    try:
        dr.mkdir(parents=True, exist_ok=True)
    except OSError as e:
        print(f"Cannot Create Directory. Error {e}")
        raise
    return str(dr)


# ---------------------------------------------------------------------------
# Parameter dump
# ---------------------------------------------------------------------------

def dump_parameter(cfg: AppConfig, logger: logging.Logger) -> None:
    logger.info(f"Sybase Server= {cfg.src_dsn} / Port = {cfg.syb_port}")
    logger.info(f"SQL Server= {cfg.tgt_dsn} / Port = {cfg.mssql_port}")
    logger.info(f"Username= {cfg.src_user} / Pwd = ******")
    logger.info("SQL Statements********:")
    for i in range(cfg.n_sql):
        logger.info(f"SQL = {cfg.sql[i]}")


# ---------------------------------------------------------------------------
# SQL generation
# ---------------------------------------------------------------------------

def gen_sql(cfg: AppConfig, logger: logging.Logger) -> None:
    cfg.key_sql = []
    cfg.tblname = {}
    cfg.tables = []

    key_sql_fld_cnt: list[int] = []

    start = cfg.tbl_start == ""
    tbl = ""
    cols: list[str] = []
    first_data_row = True

    keys_csv = Path(__file__).parent / "ConversionKeys.csv"
    with keys_csv.open(newline="") as csvfile:
        reader = csv.reader(csvfile)
        next(reader, None)  # skip header row
        for row in reader:
            if not row or not row[0].strip():
                continue
            if not start:
                if row[0] != cfg.tbl_start:
                    if tbl != row[0]:
                        tbl = row[0]
                        msg = f"[I] Skipping table {row[0]}"
                        print(msg)
                        logger.info(msg)
                    continue
                else:
                    start = True

            if first_data_row:
                tbl = row[0]
                cols = [row[1]]
                first_data_row = False
            elif tbl == row[0]:
                cols.append(row[1])
            else:
                cfg.tblname[tbl] = cols
                tbl = row[0]
                cols = [row[1]]

    if tbl and cols:
        cfg.tblname[tbl] = cols
    elif not cfg.tblname:
        print(f"[E] No tables found starting from '{cfg.tbl_start}'. Check ConversionKeys.csv.")
        return

    try:
        sql_c1 = pyodbc.connect(cfg.tgt_dsn_string)
        sql_c2 = pyodbc.connect(cfg.tgt_dsn_string)
        syb_cnxn = pyodbc.connect(cfg.src_dsn_string, timeout=0)
    except pyodbc.Error as e:
        print(f"[E] **** Database connection failed: {e}")
        print(f"      Target DSN : {cfg.tgt_dsn}")
        print(f"      Source DSN : {cfg.src_dsn}")
        print("      Check that the DSN names exist in ODBC Data Source Administrator.")
        sys.exit(1)

    with sql_c1, sql_c2, syb_cnxn:
        sql_tbl = sql_c1.cursor()
        sql_dat = sql_c2.cursor()
        syb_cursor = syb_cnxn.cursor()

        t = 0
        tblctr = 0
        tbl_keys = list(cfg.tblname.keys())

        for tbl in cfg.tblname:
            s_cols = ",".join(cfg.tblname[tbl])
            schema_sql = f"SELECT TOP 1 {s_cols} FROM {tbl}"

            try:
                sql_tbl.execute(schema_sql)
            except Exception as e:
                msg = f"[E] ***** Missing table - {tbl} skipping validations. ({e})"
                logger.error(msg)
                print(msg)
                tblctr += 1
                continue

            try:
                row = sql_tbl.fetchone()
            except Exception:
                msg = f"[E] ***** Error Reading table - {tbl} skipping validations."
                logger.error(msg)
                print(msg)
                tblctr += 1
                continue

            if row is None:
                msg = f"[E] ***** No Rows in Table  {tbl} skipping validations."
                logger.error(msg)
                print(msg)
                tblctr += 1
                continue

            cfg.key_sql.append(
                f"Select {s_cols}  FROM  {tbl} FETCH NEXT 1 ROWS ONLY"
            )
            key_sql_fld_cnt.append(len(cfg.tblname[tbl]))

            ssql = ""
            osql = ""
            for j in range(len(sql_tbl.description)):
                col = sql_tbl.description[j][0]
                schema_part, table_part = (
                    tbl_keys[tblctr].split(".", 1)
                    if "." in tbl_keys[tblctr]
                    else ("dbo", tbl_keys[tblctr])
                )
                sql_dat.execute(
                    "SELECT DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS "
                    f"WHERE TABLE_SCHEMA = '{schema_part}' "
                    f"AND TABLE_NAME = '{table_part}' "
                    f"AND UPPER(COLUMN_NAME) = UPPER('{col}')"
                )
                sdatatype = sql_dat.fetchone()
                dtype = sdatatype[0] if sdatatype else ""

                if dtype in ("int", "bigint", "smallint", "tinyint", "decimal", "numeric", "float", "real", "money", "smallmoney"):
                    ssql += f" RIGHT(REPLICATE('0',12) + CAST({col} AS VARCHAR(12)),12) +'+'+ "
                    osql += f" RIGHT(REPLICATE('0',12) + CAST({col} AS VARCHAR(12)),12) +'+'+ "
                elif dtype in ("datetime", "datetime2", "smalldatetime"):
                    ssql += f" CONVERT(VARCHAR(19),{col},120) +'+'+ "
                    osql += f" CONVERT(VARCHAR(19),{col},120) +'+'+ "
                elif dtype in ("date",):
                    ssql += f" CONVERT(VARCHAR(8),{col},112) +'+'+ "
                    osql += f" CONVERT(VARCHAR(8),{col},112) +'+'+ "
                else:
                    ssql += f" REPLACE(UPPER(COALESCE(RTRIM(CAST({col} AS VARCHAR(MAX))),'')),'_',' ') +'+'+ "
                    osql += f" REPLACE(UPPER(COALESCE(RTRIM(CAST({col} AS VARCHAR(MAX))),'')),'_',' ') +'+'+ "

            src_schema, src_table = (
                tbl_keys[tblctr].split(".", 1)
                if "." in tbl_keys[tblctr]
                else ("dbo", tbl_keys[tblctr])
            )
            tbl_structure = ""
            syb_cursor.execute(
                "SELECT c.name FROM sys.columns c "
                "JOIN sys.objects o ON c.object_id = o.object_id "
                "JOIN sys.schemas s ON o.schema_id = s.schema_id "
                f"WHERE UPPER(s.name) = UPPER('{src_schema}') "
                f"AND UPPER(o.name) = UPPER('{src_table}') "
                "ORDER BY c.column_id"
            )
            for syb_row in syb_cursor:
                tbl_structure += syb_row[0] + ","
            tbl_structure = tbl_structure[:-1]

            where_clause = (
                f"WHERE EFFECTIVEDATE={cfg.effective_date} " if cfg.effective_date else ""
            )
            cfg.syb_sql.append(
                f"SELECT {osql[:-6]} Pkey, {tbl_structure} "
                f"FROM {tbl_keys[tblctr]} A {where_clause}ORDER BY Pkey"
            )
            cfg.key_sql[t] = (
                f"SELECT {ssql[:-6]} Pkey, {tbl_structure} "
                f"FROM {tbl_keys[tblctr]} A {where_clause}ORDER BY Pkey"
            )
            cfg.tables.append(tbl_keys[tblctr])
            t += 1
            tblctr += 1


# ---------------------------------------------------------------------------
# Table validation (runs per thread)
# ---------------------------------------------------------------------------

def tbl_val(cfg: AppConfig, logger: logging.Logger, sqlindex: int) -> None:
    dt_fmt = datetime.datetime.now().strftime("%Y%m%d%H%M")
    i = sqlindex
    start_dt = datetime.datetime.now()

    d_file_name = re.sub(r"[\[\]]", "", cfg.tables[i])
    diff_path = (
        Path(cfg.f_path)
        / f"thread_{sqlindex:02d}_{d_file_name}_{dt_fmt}.csv"
    )

    logger.info(f"Generated - {diff_path}")
    logger.info(f"Starting - {cfg.key_sql[i]}")
    logger.info(f"Starting - {cfg.syb_sql[i]}")

    with (
        pyodbc.connect(cfg.src_dsn_string, timeout=0) as syb_cnxn,
        pyodbc.connect(cfg.tgt_dsn_string) as sql_cnxn,
        diff_path.open("w", newline="") as difffile,
    ):
        syb_cursor = syb_cnxn.cursor()
        sql_cursor = sql_cnxn.cursor()

        syb_cursor.execute(cfg.key_sql[i])

        try:
            syb_row = syb_cursor.fetchone()
        except Exception as e:
            logger.error(
                f"Error (Possible structure mismatch) {e} for thread {i}. "
                f"See SQL Below\n{cfg.syb_sql[i]}"
            )
            raise

        if syb_row is None:
            logger.error(f"[E] *** No data Found... for thread {i}")
            difffile.write("**No Rows Found ***")
            return

        try:
            sql_cursor.execute(cfg.syb_sql[i])
        except Exception:
            logger.error(f"[E] ***Error (Possible structure mismatch) - {cfg.tables[i]}")
            print(f"[E] ***Skipping....Error (Possible structure mismatch) {cfg.tables[i]}")
            return

        try:
            sql_row = sql_cursor.fetchone()
        except Exception as e:
            logger.error(f"[E] *** Error (Possible structure mismatch) {e}")
            return

        num_fields = len(syb_row.cursor_description)
        d_not_matching: dict[int, int] = {}
        d_fields: dict[int, tuple] = {}
        s_output = ""

        for iloop, fld in enumerate(syb_row.cursor_description):
            d_not_matching[iloop] = 0
            d_fields[iloop] = fld
            s_output += fld[0]
            if iloop < num_fields:
                s_output += ","

        logger.info("Generating Difference File Header...")
        difffile.write(s_output + "\n")
        hdr = s_output

        row_verified = 0
        row_different = 0
        miss_sql_rows = 0
        miss_syb_rows = 0
        match_rows = 0
        process_complete = False

        while not process_complete:
            if row_verified % 10000 == 0 and row_verified != 0:
                msg = f"{i} Thread -> Rows Verified {row_verified} "
                logger.info(msg)
                print(msg)

            if row_different > 2000:
                msg = (
                    f"[E] # Mismatch Rows > 2000. Terminating Validations.... "
                    f"{i} for Thread {row_verified} "
                )
                logger.error(msg)
                print(msg)
                break

            if syb_row is None and sql_row is None:
                break

            row_verified += 1

            if syb_row is None:
                s_output = f"Missing in Sybase: {sql_row[0]}" + "," * (num_fields - 1)
                difffile.write(s_output + "\n")
                sql_row = sql_cursor.fetchone()
                row_different += 1
                miss_syb_rows += 1
            elif sql_row is None:
                s_output = f"Missing in SQL: {syb_row[0]}" + "," * (num_fields - 1)
                difffile.write(s_output + "\n")
                syb_row = syb_cursor.fetchone()
                row_different += 1
                miss_sql_rows += 1
            elif syb_row[0] is None and sql_row[0] is None:
                difffile.write("Null Key value. Validation Skipped..\n")
                match_rows += 1
                sql_row = sql_cursor.fetchone()
                syb_row = syb_cursor.fetchone()
            elif syb_row[0] == sql_row[0]:
                s_output = ""
                diff = False
                for fld in range(num_fields):
                    if fld == 0:
                        s_output = str(syb_row[fld])
                    elif syb_cursor.description[fld][0] == "updt_last_tmstp":
                        pass
                    elif syb_row[fld] != sql_row[fld]:
                        col_size = syb_cursor.description[fld][3]
                        if col_size == 10:
                            if str(syb_row[fld]).rstrip() != str(sql_row[fld]).rstrip():
                                s_output += f"{syb_row[fld]} @@@ {sql_row[fld]}"
                                d_not_matching[fld] += 1
                                diff = True
                        elif col_size == 23:
                            if str(syb_row[fld])[:19] != str(sql_row[fld])[:19]:
                                s_output += f"{syb_row[fld]} @@ {sql_row[fld]}"
                                d_not_matching[fld] += 1
                                diff = True
                        elif col_size == 2147483647:
                            if repr(syb_row[fld]) != repr(sql_row[fld]):
                                s_output += f"{repr(syb_row[fld])} // {repr(sql_row[fld])}"
                                d_not_matching[fld] += 1
                                diff = True
                        else:
                            if col_size in (25, 7, 20, 30):
                                s_output += f"{syb_row[fld]} @ {sql_row[fld]}"
                                d_not_matching[fld] += 1
                                diff = True
                            elif (syb_row[fld] is None) != (sql_row[fld] is None):
                                s_output += f"{syb_row[fld]} @ {sql_row[fld]}"
                                d_not_matching[fld] += 1
                                diff = True
                            elif str(syb_row[fld]).rstrip() != str(sql_row[fld]).rstrip():
                                s_output += f"{syb_row[fld]} @ {sql_row[fld]}"
                                d_not_matching[fld] += 1
                                diff = True
                    if fld < num_fields:
                        s_output += ","

                if diff:
                    row_different += 1
                    difffile.write(repr(s_output) + "\n")

                match_rows += 1
                sql_row = sql_cursor.fetchone()
                syb_row = syb_cursor.fetchone()
            elif syb_row[0] > sql_row[0]:
                s_output = f"Missing in Sybase: {sql_row[0]}" + "," * (num_fields - 1)
                difffile.write(repr(s_output) + "\n")
                sql_row = sql_cursor.fetchone()
                row_different += 1
                miss_syb_rows += 1
            else:
                s_output = f"Missing in SQL: {syb_row[0]}" + "," * (num_fields - 1)
                difffile.write(repr(s_output) + "\n")
                syb_row = syb_cursor.fetchone()
                row_different += 1
                miss_sql_rows += 1

        difffile.write("\n\n****** Statistics of Verification ****\n")
        difffile.write(f"Total Rows,{row_verified} \n ")
        difffile.write(f"Total Rows not matching,{row_different} \n ")
        difffile.write(f"#Rows Matched ,{match_rows} \n ")
        difffile.write(f"#Rows matching in Sybase & SQL ,{match_rows - row_different} \n ")
        difffile.write(f"#Rows missing in SQL,{miss_sql_rows}  \n")
        difffile.write(f"#Rows missing in SYBASE,{miss_syb_rows}  \n")

        if sql_row is not None:
            sql_output = ""
            for sql_loop, sqlfld in enumerate(sql_row.cursor_description):
                if sqlfld[0].rstrip() != syb_row.cursor_description[sql_loop][0].rstrip():
                    sql_output += (
                        f"{syb_row.cursor_description[sql_loop][0].rstrip()} @ {sqlfld[0].rstrip()}"
                    )
                else:
                    sql_output += sqlfld[0]
                if sql_loop < num_fields:
                    sql_output += ","
            difffile.write(sql_output + "\n")
        else:
            difffile.write(hdr + "\n")

        s_output = ",".join(str(d_not_matching[k]) for k in range(num_fields))
        difffile.write(s_output + "\n")
        difffile.write("****** Statistics Complete ****\n")

    duration = datetime.datetime.now() - start_dt
    minutes, secs = divmod(duration.total_seconds(), 60)
    print(f"[I] Validation Completed for {sqlindex} ({int(minutes)}m {secs:.1f}s)")


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def main() -> None:
    cfg = AppConfig()
    dt_fmt = datetime.datetime.now().strftime("%Y%m%d%H%M")

    cfg.config_file = str(Path(__file__).parent / "LoginInfo.txt")

    if not Path(cfg.config_file).exists():
        print(f"[E] **** Config file not found: {cfg.config_file}")
        sys.exit(1)

    cfg.app_id, cfg.env = detect_app_env(cfg.config_file)
    print(f"[I] Using AppID={cfg.app_id}, Env={cfg.env} from {cfg.config_file}")

    if not read_config(cfg):
        sys.exit(1)

    cfg.tbl_start = ""       # process all tables in ConversionKeys.csv
    cfg.effective_date = ""  # no date filter applied

    cfg.f_path = create_dir(cfg)
    log_path = Path(cfg.f_path) / f"T0_logfile_{dt_fmt}.log"
    logger = setup_logging(log_path)

    dump_parameter(cfg, logger)

    if cfg.n_sql == 0:
        gen_sql(cfg, logger)
        cfg.n_sql = len(cfg.key_sql)

    if cfg.n_sql == 0:
        msg = "[E] No valid tables found to validate. Check that the tables in ConversionKeys.csv exist in the database."
        print(msg)
        logger.error(msg)
        for handler in logger.handlers:
            handler.close()
        sys.exit(1)

    for idx, tbl in enumerate(cfg.tables):
        msg = f"[I] - Thread # {idx} allocated for {tbl} table."
        print(msg)
        logger.info(msg)

    with ThreadPoolExecutor(max_workers=cfg.n_sql) as executor:
        futures = {
            executor.submit(tbl_val, cfg, logger, idx): idx
            for idx in range(cfg.n_sql)
        }
        for future in as_completed(futures):
            idx = futures[future]
            try:
                future.result()
            except Exception as exc:
                logger.error(f"Thread {idx} raised an exception: {exc}")
                print(f"[E] Thread {idx} failed: {exc}")

    for handler in logger.handlers:
        handler.close()


if __name__ == "__main__":
    main()
