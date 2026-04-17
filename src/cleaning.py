"""
Layer 2 — Data Cleaning Engine  

"""

import logging
from datetime import datetime
from pathlib import Path
from typing import Optional

import pandas as pd

log = logging.getLogger("cleaning")

NUMERIC_COLS = [
    "investment_id", "commodity_id", "cost_center_code",
    "quantity", "value_per_item_k",
    "budgeted_value_lcc_2526_k_eur", "lcc_value_option_2526_k_eur",
    "savings_lcc_2526_k_eur", "capex_already_ordered",
    "add_sap_budget_existing_wbs",
    "monthly_apr_2025", "monthly_may_2025", "monthly_jun_2025",
    "monthly_jul_2025", "monthly_aug_2025", "monthly_sep_2025",
    "monthly_oct_2025", "monthly_nov_2025", "monthly_dec_2025",
    "monthly_jan_2026", "monthly_feb_2026", "monthly_mar_2026",
    "monthly_apr_2026", "monthly_may_2026", "monthly_jun_2026",
    "monthly_jul_2026", "monthly_aug_2026", "monthly_sep_2026",
    "monthly_oct_2026", "monthly_nov_2026", "monthly_dec_2026",
    "monthly_jan_2027", "monthly_feb_2027", "monthly_mar_2027",
    "budget_q2_2025", "budget_q3_2025", "budget_q4_2025", "budget_q1_2026",
    "budget_fy_2526", "budget_q2_2026", "budget_q3_2026",
    "budget_q4_2026", "budget_q1_2027", "budget_fy_2627",
    "plan_fy_2728", "plan_fy_2829", "plan_fy_2930", "total_5y_k_eur",
    "budget_lc_2526", "budget_lc_2627", "plan_lc_2728",
    "plan_lc_2829", "plan_lc_2930",
    "budget_eur_2526", "budget_eur_2627", "plan_eur_2728",
    "plan_eur_2829", "plan_eur_2930", "total_eur_5y",
    "bms_apr_2025", "bms_may_2025", "bms_jun_2025", "bms_jul_2025",
    "bms_aug_2025", "bms_sep_2025", "bms_oct_2025", "bms_nov_2025",
    "bms_dec_2025", "bms_jan_2026", "bms_feb_2026", "bms_mar_2026",
    "bms_fy_2526", "bms_fy_2627", "bms_fy_2728",
    "bms_fy_2829", "bms_fy_2930", "bms_5y_sum", "bms_fy_2526_2627",
]

CATEGORICAL_COLS = [
    "company", "region", "plant", "cost_center_responsible",
    "investment_description", "investment_category", "customer",
    "car_model", "purchasing_l1", "purchasing_l2", "purchasing_l3",
    "purchasing_l4", "purchasing_l5", "purchasing_l6",
    "mpp_l1", "mpp_l2", "mpp_value_level", "mpp_l3_analysis",
    "tangible_intangible", "technology", "comment", "sustainability",
    "productive_non_productive", "condition_of_asset",
    "unit_of_measure", "source_of_funding", "local_currency",
    "bms_company", "bms_account_code",
]

BOOLEAN_LIKE_COLS = [
    "confirmation_required", "already_sourced", "already_sourced_2",
    "lcc_capable", "realized_lcc_2526",
]

# FIX-A: removed "0" — it's a valid numeric value and must NOT
# replace categorical fields with "Unknown".
JUNK_VALUES = {
    "please select !", "nan", "none", "n/a", "na",
    "#n/a", "#ref!", "#value!", "#div/0!", "-", "",
}

# FIX-B: investment_category-specific fixes only
# Kept separate from funding fixes to prevent cross-column contamination.
# "Other" is a valid source_of_funding value but NOT a valid investment_category.
INVESTMENT_CATEGORY_FIXES = {
    "\tEnvironment, Health & Safety (EHS)": "Environment, Health & Safety (EHS)",
    "Environment, Health & Safety (EHS) ": "Environment, Health & Safety (EHS)",
    "Other":  "Others",   # stray "Other" → canonical "Others"
    "other":  "Others",   # lowercase variant
    "OTHER":  "Others",   # uppercase variant
}

# FIX-C: source_of_funding-specific fixes only
# "Other" is valid here and must NOT be converted to "Others".
FUNDING_SOURCE_FIXES = {
    "own":        "Own",
    "OWN":        "Own",
    "leasing":    "Leasing",
    "LEASING":    "Leasing",
    "borrowings": "Borrowings",
    "BORROWINGS": "Borrowings",
    "Borrowing":  "Borrowings",
    "customer":   "Customer",
    "CUSTOMER":   "Customer",
    "other":      "Other",
    "OTHER":      "Other",
}

# FIX-D: productive_non_productive canonical map
# Maps all observed variants → exactly one of {"Productive","Non Productive","Unknown"}
PRODUCTIVE_MAP = {
    "productive":       "Productive",
    "non productive":   "Non Productive",
    "non-productive":   "Non Productive",
    "nonproductive":    "Non Productive",
    "non_productive":   "Non Productive",
    "unproductive":     "Non Productive",
    "not productive":   "Non Productive",
}


class CleaningLog:
    def __init__(self):
        self._records: list[dict] = []

    def record(self, rule, column, investment_id, before, after, note=""):
        self._records.append({
            "timestamp":     datetime.now().isoformat(),
            "rule":          rule,
            "column":        column,
            "investment_id": investment_id,
            "before":        str(before)[:200],
            "after":         str(after)[:200],
            "note":          note,
        })

    def save(self, path: str = "data/logs/cleaning_log.csv"):
        out = Path(path)
        out.parent.mkdir(parents=True, exist_ok=True)
        pd.DataFrame(self._records).to_csv(out, index=False)
        log.info("Cleaning log saved: %s (%d changes)", out, len(self._records))
        return str(out)

    @property
    def count(self):
        return len(self._records)


def _is_junk(val) -> bool:
    if val is None:
        return True
    s = str(val).strip().lower()
    return s in JUNK_VALUES or s.startswith("nan")


def clean(df: pd.DataFrame, log_path: str = "data/logs/cleaning_log.csv") -> tuple[pd.DataFrame, str]:
    df = df.copy()
    clog = CleaningLog()
    log.info("Starting cleaning — %d rows", len(df))

    # ── R1: strip whitespace first ─────────────────────────
    log.info("R1 — stripping whitespace")
    for col in df.select_dtypes(include="object").columns:
        before = df[col].copy()
        df[col] = df[col].astype(str).str.strip()
        changed = df[col] != before.astype(str).str.strip()
        for idx in df[changed].head(10).index:
            clog.record("R1_whitespace_strip", col, df.at[idx, "investment_id"],
                        before.at[idx], df.at[idx, col])

    # ── R2: fix known value inconsistencies ────────────────
    log.info("R2 — fixing known value inconsistencies")
    col_fix_map = {
        "investment_category": INVESTMENT_CATEGORY_FIXES,
        "source_of_funding":   FUNDING_SOURCE_FIXES,
    }
    for col, fixes in col_fix_map.items():
        if col not in df.columns:
            continue
        for bad, good in fixes.items():
            mask = df[col] == bad
            if mask.any():
                for idx in df[mask].index:
                    clog.record("R2_value_fix", col, df.at[idx, "investment_id"],
                                bad, good, f"standardise to '{good}'")
                df.loc[mask, col] = good
                log.info("  R2: '%s' → '%s' in '%s' (%d rows)", bad, good, col, mask.sum())

    # ── R3: investment_category "please select !" → Unknown ─
    log.info("R3 — clearing 'please select !'")
    mask = df["investment_category"].str.lower().str.contains("please select", na=False)
    if mask.any():
        for idx in df[mask].index:
            clog.record("R3_placeholder_clear", "investment_category",
                        df.at[idx, "investment_id"], df.at[idx, "investment_category"],
                        "Unknown", "placeholder value cleared")
        df.loc[mask, "investment_category"] = "Unknown"
        log.info("  R3: cleared %d placeholder values", mask.sum())

    # ── R4: numeric columns → 0 for null/junk ──────────────
    log.info("R4 — numeric nulls/junk → 0")
    num_changes = 0
    for col in NUMERIC_COLS:
        if col not in df.columns:
            continue
        original = df[col].copy()
        df[col] = pd.to_numeric(df[col], errors="coerce")
        null_mask = df[col].isna()
        if null_mask.any():
            for idx in df[null_mask].head(5).index:
                clog.record("R4_numeric_null", col, df.at[idx, "investment_id"],
                            original.at[idx], 0, "null/junk → 0")
            df.loc[null_mask, col] = 0
            num_changes += null_mask.sum()
    log.info("  R4: filled %d null/junk values with 0", num_changes)

    # ── R5: categorical columns → "Unknown" for null/junk ──
    log.info("R5 — categorical nulls/junk → 'Unknown'")
    cat_changes = 0
    for col in CATEGORICAL_COLS:
        if col not in df.columns:
            continue
        mask = df[col].apply(_is_junk)
        if mask.any():
            for idx in df[mask].head(5).index:
                clog.record("R5_categorical_null", col, df.at[idx, "investment_id"],
                            df.at[idx, col], "Unknown", "null/junk → Unknown")
            df.loc[mask, col] = "Unknown"
            cat_changes += mask.sum()
    log.info("  R5: filled %d null/junk categorical values with 'Unknown'", cat_changes)

    # ── FIX-D / R6: standardise productive_non_productive ──
    log.info("R6 — standardising productive_non_productive values")
    col = "productive_non_productive"
    if col in df.columns:
        original = df[col].copy()
        def _fix_productive(v):
            s = str(v).strip().lower()
            if s in ("unknown", "nan", "none", ""):
                return "Unknown"
            return PRODUCTIVE_MAP.get(s, v)   # keep original if not in map
        df[col] = df[col].apply(_fix_productive)
        changed = df[col] != original
        n_changed = changed.sum()
        for idx in df[changed].head(10).index:
            clog.record("R6_productive_norm", col, df.at[idx, "investment_id"],
                        original.at[idx], df.at[idx, col],
                        "standardise productive_non_productive")
        log.info("  R6: standardised %d productive_non_productive values", n_changed)

    # ── R7: boolean-like columns ───────────────────────────
    log.info("R7 — normalising boolean-like columns")
    yes_vals = {"yes", "y", "1", "true", "x"}
    no_vals  = {"no", "n", "0", "false", ""}
    for col in BOOLEAN_LIKE_COLS:
        if col not in df.columns:
            continue
        original = df[col].copy()
        df[col] = df[col].str.lower().str.strip()
        df[col] = df[col].apply(
            lambda v: "Yes" if v in yes_vals else ("No" if v in no_vals else "Unknown")
        )
        changed = df[col] != original
        for idx in df[changed].head(5).index:
            clog.record("R7_boolean_norm", col, df.at[idx, "investment_id"],
                        original.at[idx], df.at[idx, col])

    # ── R8: remove ONLY true duplicates ───────────────────
    # investment_id is NOT a unique identifier — it is a per-plant sequential counter.
    # The same ID at different plants = completely different physical investments.
    # True duplicates = same investment_id + plant + description (data entry errors).
    # Analysis showed only 2 such pairs (4 rows) in the full dataset.
    log.info("R8 — removing true duplicates (same id+plant+description)")
    dedup_cols = [c for c in ["investment_id","plant","investment_description"]
                  if c in df.columns]
    if len(dedup_cols) == 3:
        before = len(df)
        dupes = df[df.duplicated(dedup_cols, keep=False)]
        if len(dupes):
            for idx in dupes.index:
                clog.record("R8_true_duplicate", "investment_id",
                            df.at[idx, "investment_id"], "true_duplicate", "dropped",
                            f"same id+plant+description — keeping last")
            df = df.drop_duplicates(subset=dedup_cols, keep="last").copy()
            dropped = before - len(df)
            log.info("  R8: removed %d true duplicate rows. Rows remaining: %d",
                     dropped, len(df))
        else:
            log.info("  R8: no true duplicates found")
    else:
        log.warning("  R8: could not find all dedup columns, skipping")

    log.info("Cleaning complete — %d rows, %d columns, %d changes logged",
             len(df), len(df.columns), clog.count)
    log_file = clog.save(log_path)
    return df, log_file


def get_cleaning_summary(df_raw: pd.DataFrame, df_clean: pd.DataFrame) -> dict:
    return {
        "rows_raw":            len(df_raw),
        "rows_clean":          len(df_clean),
        "rows_dropped":        len(df_raw) - len(df_clean),
        "columns":             len(df_clean.columns),
        "null_numeric_filled": int(
            df_clean[
                [c for c in NUMERIC_COLS if c in df_clean.columns]
            ].eq(0).sum().sum()
        ),
        "categories_fixed": 0,
    }


if __name__ == "__main__":
    import sys
    from ingestion import ingest
    excel = "data/raw/Tarun_-_Intern_Assignment-Data.xlsx"
    df_raw = ingest(excel)
    df_clean, log_file = clean(df_raw)
    print(f"\nCleaning summary:")
    print(f"  Raw rows  : {len(df_raw):,}")
    print(f"  Clean rows: {len(df_clean):,}")
    print(f"  Audit log : {log_file}")
    print("\nCategory distribution after cleaning:")
    print(df_clean["investment_category"].value_counts())
    print("\nProductive distribution after cleaning:")
    print(df_clean["productive_non_productive"].value_counts())
    print("\nFunding source distribution after cleaning:")
    print(df_clean["source_of_funding"].value_counts())