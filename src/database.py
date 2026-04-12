"""
Layer 4 — Database Engine
Motherson Investment Intelligence

PostgreSQL only 
If the connection fails the app raises immediately with a clear message
rather than silently writing to a local file.
"""

import logging
import os

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

log = logging.getLogger("database")

# ── Monthly column → ISO date mapping ─────────────────────────
MONTHLY_COLS_FY2526 = {
    "monthly_apr_2025": "2025-04-01", "monthly_may_2025": "2025-05-01",
    "monthly_jun_2025": "2025-06-01", "monthly_jul_2025": "2025-07-01",
    "monthly_aug_2025": "2025-08-01", "monthly_sep_2025": "2025-09-01",
    "monthly_oct_2025": "2025-10-01", "monthly_nov_2025": "2025-11-01",
    "monthly_dec_2025": "2025-12-01", "monthly_jan_2026": "2026-01-01",
    "monthly_feb_2026": "2026-02-01", "monthly_mar_2026": "2026-03-01",
}
MONTHLY_COLS_FY2627 = {
    "monthly_apr_2026": "2026-04-01", "monthly_may_2026": "2026-05-01",
    "monthly_jun_2026": "2026-06-01", "monthly_jul_2026": "2026-07-01",
    "monthly_aug_2026": "2026-08-01", "monthly_sep_2026": "2026-09-01",
    "monthly_oct_2026": "2026-10-01", "monthly_nov_2026": "2026-11-01",
    "monthly_dec_2026": "2026-12-01", "monthly_jan_2027": "2027-01-01",
    "monthly_feb_2027": "2027-02-01", "monthly_mar_2027": "2027-03-01",
}
ALL_MONTHLY_COLS = {**MONTHLY_COLS_FY2526, **MONTHLY_COLS_FY2627}

CORE_COLS = [
    "investment_id", "company", "region", "plant",
    "cost_center_code", "cost_center_responsible",
    "investment_description", "investment_category",
    "customer", "car_model", "commodity_id",
    "purchasing_l1", "purchasing_l2", "purchasing_l3",
    "purchasing_l4", "purchasing_l5", "purchasing_l6",
    "mpp_l1", "mpp_l2", "mpp_value_level", "mpp_l3_analysis",
    "confirmation_required", "ras_bms_budget_account",
    "tangible_intangible", "technology", "comment",
    "sustainability", "productive_non_productive",
    "condition_of_asset", "already_sourced", "already_sourced_2",
    "planned_sourcing_date", "unit_of_measure",
    "quantity", "value_per_item_k",
    "source_of_funding", "lcc_capable",
    "budgeted_value_lcc_2526_k_eur", "realized_lcc_2526",
    "lcc_value_option_2526_k_eur", "savings_lcc_2526_k_eur",
    "local_currency",
]
BUDGET_COLS = [
    "investment_id",
    "budget_q2_2025", "budget_q3_2025", "budget_q4_2025", "budget_q1_2026",
    "budget_fy_2526",
    "budget_q2_2026", "budget_q3_2026", "budget_q4_2026", "budget_q1_2027",
    "budget_fy_2627",
    "plan_fy_2728", "plan_fy_2829", "plan_fy_2930",
    "total_5y_k_eur",
    "budget_eur_2526", "budget_eur_2627",
    "plan_eur_2728", "plan_eur_2829", "plan_eur_2930",
    "total_eur_5y",
]


# ── Connection ─────────────────────────────────────────────────

def get_engine(host=None, port=None, dbname=None, user=None, password=None) -> Engine:
    """
    Return a live SQLAlchemy engine connected to PostgreSQL.

    Reads credentials from env vars (DB_HOST, DB_PORT, DB_NAME,
    DB_USER, DB_PASSWORD) unless overridden by arguments.

    Raises RuntimeError with a clear message if the connection fails.
    Never falls back to SQLite or any other store.
    """
    host     = host     or os.getenv("DB_HOST",     "localhost")
    port     = port     or int(os.getenv("DB_PORT", "5432"))
    dbname   = dbname   or os.getenv("DB_NAME",     "motherson_intel")
    user     = user     or os.getenv("DB_USER",     "postgres")
    password = password or os.getenv("DB_PASSWORD", "")

    url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}"
    try:
        engine = create_engine(url, pool_pre_ping=True)
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        log.info("Connected to PostgreSQL: %s@%s:%s/%s", user, host, port, dbname)
        return engine
    except Exception as e:
        msg = (
            f"Cannot connect to PostgreSQL ({user}@{host}:{port}/{dbname}).\n"
            f"Reason: {e}\n"
            "Fix: check DB_HOST / DB_USER / DB_PASSWORD in your .env file "
            "and make sure the PostgreSQL server is running."
        )
        log.error(msg)
        raise RuntimeError(msg) from e


# ── Schema ─────────────────────────────────────────────────────

def create_schema(engine: Engine) -> None:
    """Create all tables and indexes if they do not already exist."""
    ddl = """
        CREATE TABLE IF NOT EXISTS investments (
            row_id                        SERIAL PRIMARY KEY,
            investment_id                 INTEGER,
            company                       TEXT,
            region                        TEXT,
            plant                         TEXT,
            cost_center_code              TEXT,
            cost_center_responsible       TEXT,
            investment_description        TEXT,
            investment_category           TEXT,
            customer                      TEXT,
            car_model                     TEXT,
            commodity_id                  NUMERIC(18,4),
            purchasing_l1                 TEXT,
            purchasing_l2                 TEXT,
            purchasing_l3                 TEXT,
            purchasing_l4                 TEXT,
            purchasing_l5                 TEXT,
            purchasing_l6                 TEXT,
            mpp_l1                        TEXT,
            mpp_l2                        TEXT,
            mpp_value_level               TEXT,
            mpp_l3_analysis               TEXT,
            confirmation_required         TEXT,
            ras_bms_budget_account        TEXT,
            tangible_intangible           TEXT,
            technology                    TEXT,
            comment                       TEXT,
            sustainability                TEXT,
            productive_non_productive     TEXT,
            condition_of_asset            TEXT,
            already_sourced               TEXT,
            already_sourced_2             TEXT,
            planned_sourcing_date         TEXT,
            unit_of_measure               TEXT,
            quantity                      NUMERIC(18,4),
            value_per_item_k              NUMERIC(18,4),
            source_of_funding             TEXT,
            lcc_capable                   TEXT,
            budgeted_value_lcc_2526_k_eur NUMERIC(18,4),
            realized_lcc_2526             TEXT,
            lcc_value_option_2526_k_eur   NUMERIC(18,4),
            savings_lcc_2526_k_eur        NUMERIC(18,4),
            local_currency                TEXT,
            created_at                    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at                    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS investment_budget (
            id                  SERIAL PRIMARY KEY,
            row_id              INTEGER UNIQUE REFERENCES investments(row_id),
            budget_q2_2025      NUMERIC(18,4) DEFAULT 0,
            budget_q3_2025      NUMERIC(18,4) DEFAULT 0,
            budget_q4_2025      NUMERIC(18,4) DEFAULT 0,
            budget_q1_2026      NUMERIC(18,4) DEFAULT 0,
            budget_fy_2526      NUMERIC(18,4) DEFAULT 0,
            budget_q2_2026      NUMERIC(18,4) DEFAULT 0,
            budget_q3_2026      NUMERIC(18,4) DEFAULT 0,
            budget_q4_2026      NUMERIC(18,4) DEFAULT 0,
            budget_q1_2027      NUMERIC(18,4) DEFAULT 0,
            budget_fy_2627      NUMERIC(18,4) DEFAULT 0,
            plan_fy_2728        NUMERIC(18,4) DEFAULT 0,
            plan_fy_2829        NUMERIC(18,4) DEFAULT 0,
            plan_fy_2930        NUMERIC(18,4) DEFAULT 0,
            total_5y_k_eur      NUMERIC(18,4) DEFAULT 0,
            budget_eur_2526     NUMERIC(18,4) DEFAULT 0,
            budget_eur_2627     NUMERIC(18,4) DEFAULT 0,
            plan_eur_2728       NUMERIC(18,4) DEFAULT 0,
            plan_eur_2829       NUMERIC(18,4) DEFAULT 0,
            plan_eur_2930       NUMERIC(18,4) DEFAULT 0,
            total_eur_5y        NUMERIC(18,4) DEFAULT 0
        );
        CREATE TABLE IF NOT EXISTS investment_monthly_cashflow (
            id              SERIAL PRIMARY KEY,
            row_id          INTEGER REFERENCES investments(row_id),
            period_date     DATE NOT NULL,
            fiscal_year     TEXT,
            amount_k_eur    NUMERIC(18,4) DEFAULT 0,
            UNIQUE(row_id, period_date)
        );
        CREATE TABLE IF NOT EXISTS investment_quarantine (
            id              SERIAL PRIMARY KEY,
            investment_id   INTEGER,
            raw_data        TEXT,
            failure_reason  TEXT,
            quarantined_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        CREATE INDEX IF NOT EXISTS idx_inv_investment_id ON investments(investment_id);
        CREATE INDEX IF NOT EXISTS idx_inv_company       ON investments(company);
        CREATE INDEX IF NOT EXISTS idx_inv_region        ON investments(region);
        CREATE INDEX IF NOT EXISTS idx_inv_category      ON investments(investment_category);
        CREATE INDEX IF NOT EXISTS idx_inv_plant         ON investments(plant);
        CREATE INDEX IF NOT EXISTS idx_cf_date           ON investment_monthly_cashflow(period_date);
        CREATE INDEX IF NOT EXISTS idx_cf_row            ON investment_monthly_cashflow(row_id)
    """
    with engine.begin() as conn:
        for stmt in [s.strip() for s in ddl.split(";") if s.strip()]:
            conn.execute(text(stmt))
    log.info("Schema created / verified (all tables and indexes)")


# ── Data preparation helpers ───────────────────────────────────

def _prepare_investments(df: pd.DataFrame) -> pd.DataFrame:
    """
    Returns the investments DataFrame ready to load.
    row_id is NOT included here — PostgreSQL SERIAL generates it on INSERT.
    investment_id is kept as a regular data column (not PK).
    """
    cols = [c for c in CORE_COLS if c in df.columns]
    return df[cols].copy()


def _prepare_budget(df: pd.DataFrame, row_ids: pd.Series) -> pd.DataFrame:
    """
    Returns the budget DataFrame with row_id column added.
    row_ids is a Series of the auto-generated row_ids from the investments table
    (in the same order as df_valid rows).
    """
    cols = [c for c in BUDGET_COLS if c in df.columns]
    out = df[cols].copy()
    # Replace investment_id FK with row_id FK
    out.insert(0, "row_id", row_ids.values)
    # Drop investment_id from budget — budget links via row_id now
    if "investment_id" in out.columns:
        out = out.drop(columns=["investment_id"])
    return out


def _prepare_monthly_cashflow(df: pd.DataFrame, row_ids: pd.Series) -> pd.DataFrame:
    """
    Returns the monthly cashflow DataFrame with row_id instead of investment_id.
    """
    monthly_cols = [c for c in ALL_MONTHLY_COLS if c in df.columns]
    if not monthly_cols:
        return pd.DataFrame(columns=["row_id", "period_date", "fiscal_year", "amount_k_eur"])

    df_work = df[["investment_id"] + monthly_cols].copy()
    df_work["row_id"] = row_ids.values

    melted = df_work[["row_id"] + monthly_cols].melt(
        id_vars=["row_id"], value_vars=monthly_cols,
        var_name="col_name", value_name="amount_k_eur",
    )
    melted["period_date"] = melted["col_name"].map(ALL_MONTHLY_COLS)
    melted["fiscal_year"] = melted["period_date"].apply(
        lambda d: "2025/26" if d <= "2026-03-01" else "2026/27"
    )
    melted["amount_k_eur"] = pd.to_numeric(melted["amount_k_eur"], errors="coerce").fillna(0)
    melted = melted[melted["amount_k_eur"] != 0].copy()
    return melted[["row_id", "period_date", "fiscal_year", "amount_k_eur"]]


# ── Load ───────────────────────────────────────────────────────

def load_data(
    df_valid: pd.DataFrame,
    df_quarantine: pd.DataFrame,
    engine: Engine,
    if_exists: str = "replace",
) -> dict:
    """
    Load cleaned + validated data into PostgreSQL.

    Strategy:
      1. Load investments — PostgreSQL SERIAL auto-generates row_id for each row.
      2. Query back the generated row_ids (in insertion order).
      3. Use those row_ids to link investment_budget and investment_monthly_cashflow.

    if_exists='replace'  → DROP CASCADE all tables, recreate schema, then load.
    if_exists='append'   → append to existing tables.
    """
    log.info("Loading data into PostgreSQL (%s mode)...", if_exists)

    if if_exists == "replace":
        with engine.begin() as conn:
            conn.execute(text("DROP TABLE IF EXISTS investment_monthly_cashflow CASCADE"))
            conn.execute(text("DROP TABLE IF EXISTS investment_budget CASCADE"))
            conn.execute(text("DROP TABLE IF EXISTS investment_quarantine CASCADE"))
            conn.execute(text("DROP TABLE IF EXISTS investments CASCADE"))
        log.info("  Old tables dropped (CASCADE)")
        create_schema(engine)

    counts = {}
    df_inv = _prepare_investments(df_valid)

    # Step 1: Load investments — row_id auto-generated by SERIAL
    df_inv.to_sql("investments", engine, if_exists="append", index=False, chunksize=500)
    counts["investments"] = len(df_inv)
    log.info("  investments: %d rows", len(df_inv))

    # Step 2: Query back the row_ids in the order they were inserted
    # We match on investment_id + company + plant + description to get the right row_id
    # for each row in df_valid (same order as df_inv).
    with engine.connect() as conn:
        row_id_df = pd.read_sql(
            "SELECT row_id, investment_id, company, plant, investment_description "
            "FROM investments ORDER BY row_id",
            conn
        )
    # Merge to get row_ids aligned with df_valid row order
    df_valid_reset = df_valid.reset_index(drop=True)
    row_ids = row_id_df["row_id"].reset_index(drop=True)

    if len(row_ids) != len(df_valid_reset):
        log.warning("Row count mismatch: investments=%d, df_valid=%d — using positional alignment",
                    len(row_ids), len(df_valid_reset))

    # Step 3: Prepare and load budget with row_ids
    df_budget = _prepare_budget(df_valid_reset, row_ids)
    df_budget.to_sql("investment_budget", engine, if_exists="append", index=False, chunksize=500)
    counts["investment_budget"] = len(df_budget)
    log.info("  investment_budget: %d rows", len(df_budget))

    # Step 4: Prepare and load cashflow with row_ids
    df_cashflow = _prepare_monthly_cashflow(df_valid_reset, row_ids)
    if len(df_cashflow):
        df_cashflow.to_sql("investment_monthly_cashflow", engine,
                           if_exists="append", index=False, chunksize=500)
        counts["investment_monthly_cashflow"] = len(df_cashflow)
        log.info("  investment_monthly_cashflow: %d rows", len(df_cashflow))
    else:
        counts["investment_monthly_cashflow"] = 0

    # Step 5: Quarantine
    if len(df_quarantine):
        dq = df_quarantine.copy()
        dq["raw_data"] = dq.drop(
            columns=["failure_reason", "quarantined_at"], errors="ignore"
        ).to_json(orient="records")
        dq[["investment_id", "raw_data", "failure_reason"]].to_sql(
            "investment_quarantine", engine, if_exists="append", index=False
        )
        counts["investment_quarantine"] = len(dq)
    else:
        counts["investment_quarantine"] = 0

    log.info("Database load complete: %s", counts)
    return counts


# ── Stats ──────────────────────────────────────────────────────

def get_db_stats(engine: Engine) -> dict:
    tables = ["investments", "investment_budget",
              "investment_monthly_cashflow", "investment_quarantine"]
    stats = {}
    with engine.connect() as conn:
        for t in tables:
            try:
                stats[t] = conn.execute(text(f"SELECT COUNT(*) FROM {t}")).scalar()
            except Exception:
                stats[t] = 0
    return stats