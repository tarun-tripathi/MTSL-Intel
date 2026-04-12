"""

Layer 3 — Validation Engine  

"""

import logging
from datetime import datetime
from pathlib import Path

import pandas as pd

log = logging.getLogger("validation")

VALID_COMPANIES = {"SMP", "SMRC", "MDRSC", "Other", "Unknown"}
VALID_REGIONS   = {
    "Germany & EE", "China", "LATAM", "Iberica",
    "France & North Africa", "Mexico", "USA", "Unknown",
}

# FIX-A: "Other" removed — cleaning.py R4 now converts "Other"→"Others"
# so only "Others" can reach this point. Keeping both would mask bugs.
VALID_CATEGORIES = {
    "Customer projects new",
    "Customer projects repeat",
    "Expansion",
    "Rationalization",
    "Replacement",
    "Environment, Health & Safety (EHS)",
    "Others",          # canonical — "Other" is fixed to "Others" in cleaning
    "Unknown",
}

VALID_FUNDING_SOURCES = {"Own", "Leasing", "Borrowings", "Customer", "Other", "Unknown"}
VALID_TANGIBLE        = {"Tangible", "Intangible", "Unknown"}

# FIX-B: matches the exact output of cleaning.py R9 standardisation
VALID_PRODUCTIVE = {"Productive", "Non Productive", "Unknown"}

VALID_MPP_LEVELS = {">=1m", ">=500k & <1m", ">=200k & <500k", "<200k", "Unknown", "No L1-L3 Given"}

FINANCIAL_COLS = [
    "budget_fy_2526", "budget_fy_2627",
    "plan_fy_2728",   "plan_fy_2829",   "plan_fy_2930",
    "total_5y_k_eur",
]


class ValidationResult:
    def __init__(self, rule_id, rule_name, severity, column):
        self.rule_id   = rule_id
        self.rule_name = rule_name
        self.severity  = severity
        self.column    = column
        self.passed    = 0
        self.failed    = 0
        self.failed_ids: list = []

    @property
    def total(self):
        return self.passed + self.failed

    @property
    def pass_rate(self) -> float:
        return (self.passed / self.total * 100) if self.total > 0 else 0.0

    def to_dict(self) -> dict:
        return {
            "rule_id":   self.rule_id,
            "rule_name": self.rule_name,
            "severity":  self.severity,
            "column":    self.column,
            "passed":    self.passed,
            "failed":    self.failed,
            "total":     self.total,
            "pass_rate": round(self.pass_rate, 2),
            "status":    "PASS" if self.failed == 0 else (
                         "SOFT FAIL" if self.severity == "SOFT" else "HARD FAIL"
            ),
        }


def _check_not_null(df, col, result):
    if col not in df.columns:
        return
    null_mask = df[col].isna() | (df[col].astype(str).str.strip() == "")
    result.passed = int((~null_mask).sum())
    result.failed = int(null_mask.sum())
    result.failed_ids = df.loc[null_mask, "investment_id"].tolist()


def _check_value_set(df, col, valid_set, result):
    if col not in df.columns:
        return
    bad_mask = ~df[col].isin(valid_set)
    result.passed = int((~bad_mask).sum())
    result.failed = int(bad_mask.sum())
    result.failed_ids = df.loc[bad_mask, "investment_id"].tolist()


def _check_non_negative(df, col, result):
    if col not in df.columns:
        return
    numeric = pd.to_numeric(df[col], errors="coerce").fillna(0)
    bad_mask = numeric < 0
    result.passed = int((~bad_mask).sum())
    result.failed = int(bad_mask.sum())
    result.failed_ids = df.loc[bad_mask, "investment_id"].tolist()


def _check_no_duplicates(df, col, result):
    dupes = df.duplicated(col, keep=False)
    result.passed = int((~dupes).sum())
    result.failed = int(dupes.sum())
    result.failed_ids = df.loc[dupes, "investment_id"].tolist()


def validate(
    df: pd.DataFrame,
    quarantine_path: str = "data/logs/quarantine.csv",
    report_path:     str = "data/logs/validation_report.csv",
) -> tuple[pd.DataFrame, pd.DataFrame, dict]:

    log.info("Starting validation — %d rows", len(df))
    results: list[ValidationResult] = []

    # ── HARD rules ─────────────────────────────────────────
    r = ValidationResult("V01", "investment_id not null", "HARD", "investment_id")
    _check_not_null(df, "investment_id", r); results.append(r)

    # V02 REMOVED: investment_id is a per-plant sequential counter, NOT a unique
    # identifier. The same ID at different plants = different physical investments.
    # Uniqueness is enforced by row_id (SERIAL PK) in the database layer.

    r = ValidationResult("V03", "company in valid set", "HARD", "company")
    _check_value_set(df, "company", VALID_COMPANIES, r); results.append(r)

    r = ValidationResult("V04", "region in valid set", "HARD", "region")
    _check_value_set(df, "region", VALID_REGIONS, r); results.append(r)

    # FIX-A: now only "Others" is valid (not "Other")
    r = ValidationResult("V05", "investment_category in valid set", "HARD", "investment_category")
    _check_value_set(df, "investment_category", VALID_CATEGORIES, r); results.append(r)

    r = ValidationResult("V06", "source_of_funding in valid set", "HARD", "source_of_funding")
    _check_value_set(df, "source_of_funding", VALID_FUNDING_SOURCES, r); results.append(r)

    # ── SOFT rules ─────────────────────────────────────────
    r = ValidationResult("V07", "budget_fy_2526 >= 0", "SOFT", "budget_fy_2526")
    _check_non_negative(df, "budget_fy_2526", r); results.append(r)

    r = ValidationResult("V08", "total_5y_k_eur >= 0", "SOFT", "total_5y_k_eur")
    _check_non_negative(df, "total_5y_k_eur", r); results.append(r)

    r = ValidationResult("V09", "tangible_intangible in valid set", "SOFT", "tangible_intangible")
    _check_value_set(df, "tangible_intangible", VALID_TANGIBLE, r); results.append(r)

    # FIX-B: uses corrected VALID_PRODUCTIVE matching cleaning R9 output
    r = ValidationResult("V10", "productive_non_productive in valid set", "SOFT", "productive_non_productive")
    _check_value_set(df, "productive_non_productive", VALID_PRODUCTIVE, r); results.append(r)

    r = ValidationResult("V11", "mpp_value_level in valid set", "SOFT", "mpp_value_level")
    _check_value_set(df, "mpp_value_level", VALID_MPP_LEVELS, r); results.append(r)

    r = ValidationResult("V12", "investment_description not null", "SOFT", "investment_description")
    _check_not_null(df, "investment_description", r); results.append(r)

    # ── Collect hard failures ───────────────────────────────
    hard_failed_ids: set = set()
    for res in results:
        if res.severity == "HARD" and res.failed_ids:
            hard_failed_ids.update(res.failed_ids)

    # ── Split valid / quarantine ────────────────────────────
    if hard_failed_ids:
        quarantine_mask = df["investment_id"].isin(hard_failed_ids)
        df_quarantine = df[quarantine_mask].copy()
        df_valid      = df[~quarantine_mask].copy()

        def _reason(row):
            reasons = []
            for res in results:
                if res.severity == "HARD" and row["investment_id"] in res.failed_ids:
                    reasons.append(f"{res.rule_id}: {res.rule_name}")
            return "; ".join(reasons)

        df_quarantine["failure_reason"] = df_quarantine.apply(_reason, axis=1)
        df_quarantine["quarantined_at"] = datetime.now().isoformat()
    else:
        df_quarantine = pd.DataFrame()
        df_valid      = df.copy()

    # ── Save outputs ─────────────────────────────────────────
    Path(quarantine_path).parent.mkdir(parents=True, exist_ok=True)
    if len(df_quarantine):
        df_quarantine.to_csv(quarantine_path, index=False)
        log.warning("Quarantined %d rows → %s", len(df_quarantine), quarantine_path)

    report_df = pd.DataFrame([r.to_dict() for r in results])
    report_df.to_csv(report_path, index=False)

    overall_pass_rate = sum(r.passed for r in results) / max(sum(r.total for r in results), 1) * 100
    report = {
        "total_rows":        len(df),
        "valid_rows":        len(df_valid),
        "quarantine_rows":   len(df_quarantine),
        "rules_run":         len(results),
        "rules_passed":      sum(1 for r in results if r.failed == 0),
        "rules_failed":      sum(1 for r in results if r.failed > 0),
        "overall_pass_rate": round(overall_pass_rate, 2),
        "results":           [r.to_dict() for r in results],
        "report_path":       report_path,
        "quarantine_path":   quarantine_path,
    }

    log.info(
        "Validation complete — %d valid, %d quarantined, %d/%d rules passed (%.1f%%)",
        len(df_valid), len(df_quarantine),
        report["rules_passed"], report["rules_run"], overall_pass_rate,
    )
    return df_valid, df_quarantine, report


if __name__ == "__main__":
    import sys, os
    sys.path.insert(0, os.path.dirname(__file__))
    from ingestion import ingest
    from cleaning   import clean
    df_raw   = ingest("data/raw/Tarun_-_Intern_Assignment-Data.xlsx")
    df_clean, _ = clean(df_raw)
    df_valid, df_quar, report = validate(df_clean)
    print(f"\nValidation report:")
    print(f"  Total rows      : {report['total_rows']:,}")
    print(f"  Valid rows      : {report['valid_rows']:,}")
    print(f"  Quarantined     : {report['quarantine_rows']:,}")
    print(f"  Rules passed    : {report['rules_passed']}/{report['rules_run']}")
    print(f"  Pass rate       : {report['overall_pass_rate']}%")
    for r in report["results"]:
        print(f"  {r['status']:18s}  {r['rule_id']} — {r['rule_name']} ({r['passed']}/{r['total']})")