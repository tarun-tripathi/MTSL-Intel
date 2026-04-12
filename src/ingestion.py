"""

Layer 1 — Ingestion Engine

Reads the Motherson Excel file and produces a clean raw
DataFrame ready for the cleaning layer.

Key decisions documented here:
  - header is at row 12 (0-indexed); rows 0-11 are Motherson
    boilerplate (instructions, region codes, validation notes)
  - 125 columns are renamed to clean snake_case identifiers
  - rows with non-numeric ID are metadata rows and are dropped
  - the two fully-empty columns (123, 124) are dropped
"""

import logging
import re
from pathlib import Path

import pandas as pd
import yaml

# ── Logging setup ──────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("ingestion")


# ── Column name map ────────────────────────────────────────
# Maps original messy Excel header text → clean snake_case name.
# Every column in the file is accounted for.
COLUMN_MAP = {
    # Core identifiers
    0:  "investment_id",
    1:  "company",
    2:  "region",
    3:  "plant",
    4:  "cost_center_code",
    5:  "cost_center_responsible",
    6:  "investment_description",
    7:  "investment_category",
    8:  "customer",
    9:  "car_model",
    10: "commodity_id",
    # Purchasing commodity hierarchy
    11: "purchasing_l1",
    12: "purchasing_l2",
    13: "purchasing_l3",
    14: "purchasing_l4",
    15: "purchasing_l5",
    16: "purchasing_l6",
    # MPP (Material Planning Process) hierarchy
    17: "mpp_l1",
    18: "mpp_l2",
    19: "mpp_value_level",
    20: "mpp_l3_analysis",
    21: "confirmation_required",
    # Financial classification
    22: "ras_bms_budget_account",
    23: "tangible_intangible",
    24: "technology",
    25: "comment",
    26: "sustainability",
    27: "productive_non_productive",
    28: "condition_of_asset",
    29: "already_sourced",
    30: "already_sourced_2",
    31: "planned_sourcing_date",
    32: "unit_of_measure",
    33: "quantity",
    34: "value_per_item_k",
    35: "source_of_funding",
    36: "lcc_capable",
    37: "budgeted_value_lcc_2526_k_eur",
    38: "realized_lcc_2526",
    39: "lcc_value_option_2526_k_eur",
    40: "savings_lcc_2526_k_eur",
    41: "existing_ras_number",
    42: "capex_already_ordered",
    43: "add_sap_budget_existing_wbs",
    # Monthly cashflow FY 2025/26 (Apr 2025 – Mar 2026)
    44: "monthly_apr_2025",
    45: "monthly_may_2025",
    46: "monthly_jun_2025",
    47: "monthly_jul_2025",
    48: "monthly_aug_2025",
    49: "monthly_sep_2025",
    50: "monthly_oct_2025",
    51: "monthly_nov_2025",
    52: "monthly_dec_2025",
    53: "monthly_jan_2026",
    54: "monthly_feb_2026",
    55: "monthly_mar_2026",
    # Quarterly summary FY 2025/26
    56: "budget_q2_2025",
    57: "budget_q3_2025",
    58: "budget_q4_2025",
    59: "budget_q1_2026",
    60: "budget_fy_2526",
    # Monthly cashflow FY 2026/27 (Apr 2026 – Mar 2027)
    61: "monthly_apr_2026",
    62: "monthly_may_2026",
    63: "monthly_jun_2026",
    64: "monthly_jul_2026",
    65: "monthly_aug_2026",
    66: "monthly_sep_2026",
    67: "monthly_oct_2026",
    68: "monthly_nov_2026",
    69: "monthly_dec_2026",
    70: "monthly_jan_2027",
    71: "monthly_feb_2027",
    72: "monthly_mar_2027",
    # Quarterly summary FY 2026/27
    73: "budget_q2_2026",
    74: "budget_q3_2026",
    75: "budget_q4_2026",
    76: "budget_q1_2027",
    77: "budget_fy_2627",
    # Multi-year plan
    78: "plan_fy_2728",
    79: "plan_fy_2829",
    80: "plan_fy_2930",
    81: "total_5y_k_eur",
    # Local currency section
    82: "local_currency",
    83: "currency_flag",
    84: "budget_lc_2526",
    85: "budget_lc_2627",
    86: "plan_lc_2728",
    87: "plan_lc_2829",
    88: "plan_lc_2930",
    # EUR converted section
    89: "budget_eur_2526",
    90: "budget_eur_2627",
    91: "plan_eur_2728",
    92: "plan_eur_2829",
    93: "plan_eur_2930",
    94: "total_eur_5y",
    95: "empty_col_95",
    96: "col_flag_96",
    97: "bms_company",
    98: "bms_account_code",
    99: "col_flag_99",
    100: "cost_center_bms",
    # BMS monthly cashflow (duplicate of cols 44-55 in EUR)
    101: "bms_apr_2025",
    102: "bms_may_2025",
    103: "bms_jun_2025",
    104: "bms_jul_2025",
    105: "bms_aug_2025",
    106: "bms_sep_2025",
    107: "bms_oct_2025",
    108: "bms_nov_2025",
    109: "bms_dec_2025",
    110: "bms_jan_2026",
    111: "bms_feb_2026",
    112: "bms_mar_2026",
    # BMS annual totals
    113: "bms_fy_2526",
    114: "bms_fy_2627",
    115: "bms_fy_2728",
    116: "bms_fy_2829",
    117: "bms_fy_2930",
    118: "bms_5y_sum",
    119: "bms_fy_2526_2627",
    120: "sorting_key",
    121: "internal_col_121",
    122: "internal_col_122",
    123: "empty_col_123",
    124: "empty_col_124",
}


def _slugify(text: str) -> str:
    """Convert any string to a clean snake_case identifier."""
    text = str(text).strip().lower()
    text = re.sub(r"[\n\r\t]+", " ", text)           # newlines → space
    text = re.sub(r"[^a-z0-9\s_]", "", text)          # remove special chars
    text = re.sub(r"\s+", "_", text.strip())           # spaces → underscores
    text = re.sub(r"_+", "_", text)                    # collapse double underscores
    return text or "unnamed_col"


def load_config(config_path: str = "config.yaml") -> dict:
    """Load project config from YAML."""
    with open(config_path) as f:
        return yaml.safe_load(f)


def ingest(
    excel_path: str,
    header_row: int = 12,
    config_path: str = "config.yaml",
) -> pd.DataFrame:
    """
    Read the Motherson Excel file and return a clean raw DataFrame.

    Steps:
      1. Read with header at row 12 (skip Motherson boilerplate)
      2. Rename all 125 columns using COLUMN_MAP
      3. Drop rows where investment_id is not numeric (metadata rows)
      4. Cast investment_id to int
      5. Drop the two fully-empty columns
      6. Log row/column counts

    Args:
        excel_path:  Path to the .xlsx file.
        header_row:  0-indexed row number of the real header (default 12).
        config_path: Path to config.yaml (used for logging dir).

    Returns:
        pd.DataFrame with 5,172 rows and clean column names.
    """
    path = Path(excel_path)
    if not path.exists():
        raise FileNotFoundError(f"Excel file not found: {path}")

    log.info("Reading Excel file: %s", path)
    df_raw = pd.read_excel(
        path,
        sheet_name="Database",
        header=header_row,
        engine="openpyxl",
        dtype=str,          # read everything as string first — clean in layer 2
    )
    log.info("Raw shape: %d rows × %d cols", *df_raw.shape)

    # ── Step 1: rename columns by position ──────────────────
    # map by position (not by text) because the header text
    # contains newlines, unicode, and inconsistent whitespace.
    new_cols = {}
    for i, col in enumerate(df_raw.columns):
        if i in COLUMN_MAP:
            new_cols[col] = COLUMN_MAP[i]
        else:
            new_cols[col] = f"col_{i:03d}"
    df_raw.rename(columns=new_cols, inplace=True)
    log.info("Columns renamed to snake_case identifiers.")

    # ── Step 2: filter to real data rows ────────────────────
    # Only rows where investment_id is a pure integer string
    mask = df_raw["investment_id"].str.strip().str.match(r"^\d+$", na=False)
    df = df_raw[mask].copy()
    dropped_meta = len(df_raw) - len(df)
    log.info(
        "Dropped %d non-data rows (metadata / instructions). Remaining: %d",
        dropped_meta, len(df),
    )

    # ── Step 3: cast ID ──────────────────────────────────────
    df["investment_id"] = df["investment_id"].str.strip().astype(int)

    # ── Step 4: drop fully-empty columns ────────────────────
    empty_cols = [c for c in ["empty_col_123", "empty_col_124"] if c in df.columns]
    df.drop(columns=empty_cols, inplace=True)
    log.info("Dropped %d fully-empty columns: %s", len(empty_cols), empty_cols)

    log.info(
        "Ingestion complete — %d rows × %d columns ready for cleaning.",
        *df.shape,
    )
    return df


def save_raw_parquet(df: pd.DataFrame, output_dir: str = "data/processed") -> str:
    """
    Persist the raw (pre-cleaning) DataFrame as Parquet.
    Useful for debugging and re-running cleaning without re-reading Excel.
    """
    out = Path(output_dir) / "raw_ingested.parquet"
    out.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(out, index=False)
    log.info("Raw data saved to: %s", out)
    return str(out)


# ── CLI entry point ──────────────────────────────────────────
if __name__ == "__main__":
    import sys
    excel = sys.argv[1] if len(sys.argv) > 1 else "data/raw/Tarun_-_Intern_Assignment-Data.xlsx"
    df = ingest(excel)
    save_raw_parquet(df)
    print(f"\nIngestion summary:")
    print(f"  Rows    : {len(df):,}")
    print(f"  Columns : {len(df.columns)}")
    print(f"\nFirst 5 rows (key columns):")
    key_cols = ["investment_id", "company", "region", "plant", "investment_category", "budget_fy_2526"]
    print(df[key_cols].head().to_string(index=False))
