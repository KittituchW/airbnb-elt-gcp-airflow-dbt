from __future__ import annotations
import io, os, re, shutil, logging
from datetime import datetime

import pandas as pd
from airflow.decorators import dag, task
from airflow.exceptions import AirflowException
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.hooks.base import BaseHook
import requests
from airflow.models import Variable

log = logging.getLogger(__name__)

# ========= CONFIG =========
# I keep all data folders under /home/airflow/gcs/data on Composer.
AIRFLOW_DATA = "/home/airflow/gcs/data"
DIMENSIONS   = os.path.join(AIRFLOW_DATA, "dimensions")  # 4 dim CSVs live here
FACTS        = os.path.join(AIRFLOW_DATA, "facts")       # month CSVs live here
ARCHIVES     = os.path.join(AIRFLOW_DATA, "archives")    # I move processed files here

PG_CONN_ID = "postgres"  # Airflow connection id to Postgres

# Target tables in Postgres (schema-qualified). 
BRONZE_FACT_TABLE = "bronze.r_f_listing"
DIM_FILE_TO_TABLE = {
    "2016Census_G01_NSW_LGA.csv": "bronze.r_d_2016census_g01_nsw_lga",
    "2016Census_G02_NSW_LGA.csv": "bronze.r_d_2016census_g02_nsw_lga",
    "NSW_LGA_CODE.csv":           "bronze.r_d_nsw_lga_code",
    "NSW_LGA_SUBURB.csv":         "bronze.r_d_nsw_lga_suburb",
}

import re as _re
FACT_FILE_RE = _re.compile(r"^(0[1-9]|1[0-2])_2020\.csv$|^(0[1-4])_2021\.csv$")

DEFAULT_ARGS = {"owner": "Kittituch", "retries": 0, "depends_on_past": False}

# ========= HELPERS =========
def _ensure_dir(path: str, label: str):
    """Make sure a folder exists before I try to read from it."""
    if not os.path.isdir(path):
        raise AirflowException(f"[BDE] {label} directory not found: {path}")
    log.info("[BDE] %s OK: %s", label, path)

def _table_exists(cur, fqtn: str) -> bool:
    """Check a fully-qualified table like schema.table exists."""
    if "." not in fqtn:
        raise AirflowException(f"[BDE] Invalid table ref: {fqtn}")
    schema, table = fqtn.split(".", 1)
    schema, table = schema.strip('"').lower(), table.strip('"').lower()
    cur.execute(
        "SELECT 1 FROM information_schema.tables "
        "WHERE table_schema=%s AND table_name=%s LIMIT 1;",
        (schema, table),
    )
    return cur.fetchone() is not None

def _db_cols(cur, fqtn: str) -> list[str]:
    """Get the ordered column list from Postgres for COPY alignment."""
    schema, table = fqtn.split(".", 1)
    schema, table = schema.strip('"').lower(), table.strip('"').lower()
    cur.execute(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_schema=%s AND table_name=%s ORDER BY ordinal_position;",
        (schema, table),
    )
    return [r[0] for r in cur.fetchall()]

def _db_col_types(cur, fqtn: str) -> dict[str, str]:
    """Return {column_name: data_type} so I can validate Bronze text dates."""
    schema, table = fqtn.split(".", 1)
    schema, table = schema.strip('"').lower(), table.strip('"').lower()
    cur.execute(
        """
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_schema=%s AND table_name=%s;
        """,
        (schema, table),
    )
    return {name: dtype for name, dtype in cur.fetchall()}

def _read_csv_str(path: str) -> pd.DataFrame:
    """Read a CSV as all strings; fail fast if missing/empty."""
    if not os.path.isfile(path) or os.path.getsize(path) == 0:
        raise AirflowException(f"[BDE] CSV missing/empty: {path}")
    return pd.read_csv(path, dtype=str)

def _prep_dim_df(fname: str, df: pd.DataFrame, db_cols: list[str]) -> pd.DataFrame:
    """Light cleanup per file so the CSV matches table columns exactly."""
    name = os.path.basename(fname).upper()
    if name == "NSW_LGA_SUBURB.CSV":
        # I only need the first two columns, and I normalise whitespace.
        df = df.iloc[:, :2].copy()
        df.columns = ["lga_name", "suburb_name"]
        df = df.fillna("").applymap(lambda x: x.strip() if isinstance(x, str) else x)
        if len(db_cols) != 2:
            raise AirflowException("[BDE] DB expects 2 cols for r_d_nsw_lga_suburb")
    elif name == "NSW_LGA_CODE.CSV":
        df = df.iloc[:, :2].copy()
        df.columns = ["lga_code", "lga_name"]
        df = df.fillna("").applymap(lambda x: x.strip() if isinstance(x, str) else x)
        if len(db_cols) != 2:
            raise AirflowException("[BDE] DB expects 2 cols for r_d_nsw_lga_code")
    else:
        # Other dim files must match the DB column count 1:1.
        if df.shape[1] != len(db_cols):
            raise AirflowException(
                f"[BDE] Column mismatch {fname}: CSV={df.shape[1]} DB={len(db_cols)}"
            )
    # Double-check after my prep I still match exactly.
    if df.shape[1] != len(db_cols):
        raise AirflowException(
            f"[BDE] After prep, mismatch for {fname}: CSV={df.shape[1]} DB={len(db_cols)}"
        )
    return df

# ---- Strict day-first -> ISO normaliser (no fallback) ----
def _to_iso_date_series_dayfirst(s: pd.Series) -> pd.Series:
    """
    I accept only d/m/Y, d-m-Y, d.m.Y, or already ISO Y-m-d.
    Everything else becomes empty string so Bronze keeps clean text dates.
    """
    import re
    s = s.astype(str).replace({"nan": "", "NaN": "", "None": ""}).str.strip()
    s_norm = s.str.replace(r"[./]", "-", regex=True)  # normalise separators to '-'

    def to_iso(x: str) -> str:
        if not x:
            return ""
        # Already ISO: Y-m-d
        m_iso = re.fullmatch(r"(\d{4})-(\d{1,2})-(\d{1,2})", x)
        if m_iso:
            Y, M, D = map(int, m_iso.groups())
            if 1 <= M <= 12 and 1 <= D <= 31:
                return f"{Y:04d}-{M:02d}-{D:02d}"
            return ""
        # Day-first d-m-Y
        m_dmy = re.fullmatch(r"(\d{1,2})-(\d{1,2})-(\d{4})", x)
        if m_dmy:
            D, M, Y = map(int, m_dmy.groups())
            if 1 <= M <= 12 and 1 <= D <= 31:
                return f"{Y:04d}-{M:02d}-{D:02d}"
            return ""
        return ""

    return s_norm.map(to_iso)

def _clean_facts(df: pd.DataFrame) -> pd.DataFrame:
    """Minimal cleaning for Bronze: text dates + numeric price only."""
    df = df.astype(str).replace({"nan": "", "NaN": "", "None": ""})
    for c in ["SCRAPED_DATE", "HOST_SINCE"]:
        if c in df.columns:
            df[c] = _to_iso_date_series_dayfirst(df[c])  # keep ISO text
    if "PRICE" in df.columns:
        df["PRICE"] = df["PRICE"].str.replace(r"[^0-9.\-]", "", regex=True)
    return df

def _align_to_db_columns(df: pd.DataFrame, db_cols: list[str]) -> pd.DataFrame:
    """Make sure the CSV columns line up 1:1 with the DB columns before COPY."""
    for c in db_cols:
        if c not in df.columns:
            if c.upper() in df.columns:
                df[c] = df[c.upper()]
            elif c.lower() in df.columns:
                df[c] = df[c.lower()]
            else:
                df[c] = ""
    return df.reindex(columns=db_cols, fill_value="")

def _copy_with_explicit_columns(cur, table: str, df: pd.DataFrame, db_cols: list[str]):
    """COPY with an explicit column list so case/order always match the table."""
    buf = io.StringIO()
    df.to_csv(buf, index=False, header=False, na_rep="")
    buf.seek(0)
    col_list_sql = ", ".join(f'"{c}"' for c in db_cols)
    cur.copy_expert(
        f"COPY {table} ({col_list_sql}) FROM STDIN WITH (FORMAT CSV, NULL '\'')",
        buf
    )

def _assert_bronze_text_dates(cur):
    """Bronze must keep date columns as TEXT so Postgres never silently re-parses."""
    types = _db_col_types(cur, BRONZE_FACT_TABLE)
    bad = []
    for col in ["SCRAPED_DATE", "HOST_SINCE"]:
        t = types.get(col.lower())
        if t is None:
            t = types.get(col)
        if t is None:
            bad.append(f"{col} <missing>")
        elif t.lower() not in ("text", "character varying", "varchar"):
            bad.append(f"{col}={t}")
    if bad:
        raise AirflowException(
            "[BDE] Bronze table must keep date columns as TEXT. Found: " + ", ".join(bad)
        )
    log.info("[BDE] Bronze date columns are TEXT: OK")

# ========= DAG =========
@dag(
    dag_id="load_to_bronze_parallel_dbt_2",
    start_date=datetime(2025, 10, 1),
    schedule=None,
    catchup=False,
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=["bronze", "airbnb", "postgres"],
)
def load_to_bronze_local():

    @task
    def step_1_preflight() -> str:
        """I check folders exist and I can connect to Postgres."""
        log.info("[BDE][STEP 1/7] Preflight: check dirs & connection")
        _ensure_dir(AIRFLOW_DATA, "AIRFLOW_DATA")
        _ensure_dir(DIMENSIONS, "DIMENSIONS")
        _ensure_dir(FACTS, "FACTS")
        os.makedirs(ARCHIVES, exist_ok=True)

        # Validate Airflow connection and log DB version
        BaseHook.get_connection(PG_CONN_ID)
        pg = PostgresHook(postgres_conn_id=PG_CONN_ID)
        with pg.get_conn() as db, db.cursor() as cur:
            cur.execute("SELECT current_database(), version();")
            dbn, v = cur.fetchone()
            log.info("[BDE] Connected to %s (%s)", dbn, v)
        return "ok"

    @task
    def step_2_assert_tables(_: str) -> str:
        """I make sure all required tables are present and Bronze date types are TEXT."""
        log.info("[BDE][STEP 2/7] Assert required tables exist + Bronze types")
        required = [BRONZE_FACT_TABLE, *DIM_FILE_TO_TABLE.values()]
        pg = PostgresHook(postgres_conn_id=PG_CONN_ID)
        with pg.get_conn() as conn, conn.cursor() as cur:
            missing = [t for t in required if not _table_exists(cur, t)]
            if missing:
                raise AirflowException(f"[BDE] Missing tables: {missing}")
            _assert_bronze_text_dates(cur)
        return "tables_ok"

    # ---- DIMENSION BRANCH ----
    @task
    def step_3_list_dimensions(_: str) -> list[tuple[str, str]]:
        """I discover which dimension CSVs are present."""
        log.info("[BDE][STEP 3/7] List dimension CSVs")
        pairs: list[tuple[str, str]] = []
        for fname, table in DIM_FILE_TO_TABLE.items():
            fpath = os.path.join(DIMENSIONS, fname)
            if os.path.isfile(fpath):
                pairs.append((fpath, table))
            else:
                log.warning("[BDE] Dimension file missing: %s", fpath)
        if not pairs:
            log.warning("[BDE] No dimension files found")
        return pairs

    @task
    def step_4_load_dimensions(pairs: list[tuple[str, str]]) -> str:
        """For each found dim file, I TRUNCATE the table and COPY load fresh rows."""
        log.info("[BDE][STEP 4/7] Load dimensions (truncate -> copy)")
        if not pairs:
            log.info("[BDE] No dimension files to load")
            return "no_dims"
        pg = PostgresHook(postgres_conn_id=PG_CONN_ID)
        with pg.get_conn() as conn, conn.cursor() as cur:
            for fpath, table in pairs:
                if not _table_exists(cur, table):
                    raise AirflowException(f"[BDE] Table not found: {table}")
                df_raw  = _read_csv_str(fpath)
                cols_db = _db_cols(cur, table)
                df      = _prep_dim_df(os.path.basename(fpath), df_raw, cols_db)

                log.info("[BDE] Truncating %s", table)
                cur.execute(f"TRUNCATE TABLE {table};")

                log.info("[BDE] Loading %s -> %s (%d rows)", os.path.basename(fpath), table, len(df))
                _copy_with_explicit_columns(cur, table, _align_to_db_columns(df, cols_db), cols_db)
                conn.commit()
        return "dims_loaded"

    # ---- FACT BRANCH ----
    @task
    def step_5_list_facts(_: str) -> list[str]:
        """I list monthly fact CSVs that match my strict filename pattern."""
        log.info("[BDE][STEP 5/7] List fact CSVs")
        files = [os.path.join(FACTS, f) for f in sorted(os.listdir(FACTS)) if FACT_FILE_RE.match(f)]
        if not files:
            log.warning("[BDE] No fact files matched pattern")
        else:
            log.info("[BDE] Fact files: %s", [os.path.basename(x) for x in files])
        return files

    @task
    def step_6_load_facts(files: list[str]) -> str:
        """I clean and load each month into Bronze (no truncate), then archive the CSV."""
        log.info("[BDE][STEP 6/7] Load facts (clean -> insert -> archive)")
        if not files:
            log.info("[BDE] No fact files to load")
            return "no_facts"

        pg = PostgresHook(postgres_conn_id=PG_CONN_ID)
        with pg.get_conn() as conn, conn.cursor() as cur:
            if not _table_exists(cur, BRONZE_FACT_TABLE):
                raise AirflowException(f"[BDE] Table not found: {BRONZE_FACT_TABLE}")

            cols_db = _db_cols(cur, BRONZE_FACT_TABLE)

            for fpath in files:
                fname = os.path.basename(fpath)
                month_year = fname.replace(".csv", "")

                # Skip the file if this month is already present (idempotent per file)
                cur.execute(f"SELECT 1 FROM {BRONZE_FACT_TABLE} WHERE month_year=%s LIMIT 1;", (month_year,))
                if cur.fetchone():
                    log.info("[BDE] Skip %s (month_year exists); archiving", fname)
                    dst_dir = os.path.join(ARCHIVES, "facts")
                    os.makedirs(dst_dir, exist_ok=True)
                    dst = os.path.join(dst_dir, fname)
                    if os.path.exists(dst):
                        base, ext = os.path.splitext(fname)
                        dst = os.path.join(dst_dir, f"{base}_{datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')}{ext}")
                    try:
                        shutil.move(fpath, dst)
                        log.info("[BDE] Archived %s (already loaded)", fname)
                    except Exception as e:
                        log.warning("[BDE] Archive move failed %s: %s", fname, e)
                    continue

                # Read -> enrich -> clean for Bronze
                df = _read_csv_str(fpath)
                df["month_year"] = month_year          # keep identifier consistent with DB
                df = _clean_facts(df)                   # ISO text dates + numeric price

                # Guard natural PK (listing_id, scraped_date)
                before = len(df)
                df = df[(df["LISTING_ID"].astype(str).str.strip() != "") &
                        (df["SCRAPED_DATE"].astype(str).str.strip() != "")]
                dropped = before - len(df)
                if dropped:
                    log.warning("[BDE] Dropped %d rows with NULL PK fields (LISTING_ID/SCRAPED_DATE)", dropped)

                # Align to DB columns and COPY with explicit column list
                df_aligned = _align_to_db_columns(df, cols_db)

                # Quick sanity log of parsed dates so I can spot format issues fast
                sample_scraped = df_aligned.loc[df_aligned["SCRAPED_DATE" ] != "", "SCRAPED_DATE"].head(5).tolist()
                sample_host    = df_aligned.loc[df_aligned["HOST_SINCE"   ] != "", "HOST_SINCE"  ].head(5).tolist()
                log.info("[BDE] Sample parsed SCRAPED_DATE: %s", sample_scraped)
                log.info("[BDE] Sample parsed HOST_SINCE  : %s", sample_host)

                log.info("[BDE] Inserting %s -> %s (%d rows)", fname, BRONZE_FACT_TABLE, len(df_aligned))
                _copy_with_explicit_columns(cur, BRONZE_FACT_TABLE, df_aligned, cols_db)
                conn.commit()

                # Optional: refresh stats to help planners after big COPY
                try:
                    cur.execute(f"ANALYZE {BRONZE_FACT_TABLE};")
                except Exception as e:
                    log.warning("[BDE] ANALYZE failed (non-fatal): %s", e)

                # Archive after success (avoid overwriting existing archive)
                dst_dir = os.path.join(ARCHIVES, "facts")
                os.makedirs(dst_dir, exist_ok=True)
                dst = os.path.join(dst_dir, fname)
                if os.path.exists(dst):
                    base, ext = os.path.splitext(fname)
                    dst = os.path.join(dst_dir, f"{base}_{datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')}{ext}")
                try:
                    shutil.move(fpath, dst)
                    log.info("[BDE] Archived %s", fname)
                except Exception as e:
                    log.warning("[BDE] Archive move failed %s: %s", fname, e)

        return "facts_loaded"

    # ---- BARRIER (JOIN BOTH BRANCHES) ----
    @task
    def step_6b_barrier(_dims: str, _facts: str) -> str:
        """Simple barrier to make sure both branches completed before I trigger dbt."""
        log.info("[BDE][STEP 6b/7] Barrier reached: dims=%s, facts=%s", _dims, _facts)
        return "ready_for_dbt"

    @task
    def step_7_trigger_dbt(_: str) -> dict:
        """
        I trigger a dbt Cloud job via POST /api/v2/accounts/{acct}/jobs/{job}/run/.
        The host, account, job id, and token come from Airflow Variables.
        """
        dbt_cloud_url = Variable.get("DBT_CLOUD_URL").replace("https://", "").replace("http://", "").strip("/")
        account_id = Variable.get("DBT_CLOUD_ACCOUNT_ID")
        job_id     = Variable.get("DBT_CLOUD_JOB_ID")
        token      = Variable.get("DBT_CLOUD_API_TOKEN")

        url = f"https://{dbt_cloud_url}/api/v2/accounts/{account_id}/jobs/{job_id}/run/"
        headers = {"Authorization": f"Token {token}", "Content-Type": "application/json"}
        payload = {"cause": "Triggered via Airflow after Bronze load"}

        log.info("[DBT] Triggering dbt Cloud job: %s (acct=%s, job=%s)", url, account_id, job_id)
        try:
            resp = requests.post(url, headers=headers, json=payload, timeout=60)
            resp.raise_for_status()
        except requests.RequestException as e:
            log.error("[DBT] Trigger failed: %s", e)
            raise AirflowException(str(e))

        result = resp.json()
        run_id = (result.get("data") or {}).get("id")
        log.info("[DBT] Triggered dbt Cloud run_id=%s", run_id)
        return {"status_code": resp.status_code, "run_id": run_id, "raw": result}

    # --- Wiring (parallel branches + join) ---
    # I run preflight, check tables, then branch: dims and facts load in parallel.
    pre      = step_1_preflight()
    check    = step_2_assert_tables(pre)

    # Parallel discover
    dim_list  = step_3_list_dimensions(check)
    fact_list = step_5_list_facts(check)

    # Parallel loads
    dim_load  = step_4_load_dimensions(dim_list)
    fact_load = step_6_load_facts(fact_list)

    # Join before dbt
    barrier   = step_6b_barrier(dim_load, fact_load)
    _dbt      = step_7_trigger_dbt(barrier)

# Expose the DAG object
dag = load_to_bronze_local()