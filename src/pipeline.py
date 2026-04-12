"""

Pipeline Runner — Orchestrates All Layers 1 → 4

Run this once to: ingest → clean → validate → load to DB.
After this, the Streamlit app (app.py) uses the database.

Usage:
    cd motherson_intel
    python src/pipeline.py

    # With custom Excel path:
    python src/pipeline.py --excel data/raw/myfile.xlsx

    # Force re-load (drop and recreate tables):
    python src/pipeline.py --reload
"""

import argparse
import logging
import sys
import time
from pathlib import Path

# ── Setup path so imports work from project root ────────────
sys.path.insert(0, str(Path(__file__).parent))

from dotenv import load_dotenv
load_dotenv()

from ingestion  import ingest, save_raw_parquet
from cleaning   import clean, get_cleaning_summary
from validation import validate
from database   import get_engine, load_data, get_db_stats

# ── Logging ─────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("data/logs/pipeline.log"),
    ],
)
log = logging.getLogger("pipeline")


def run_pipeline(
    excel_path: str = "data/raw/Tarun_-_Intern_Assignment-Data.xlsx",
    reload:     bool = True,
) -> dict:
    """
    Execute the full ingestion → cleaning → validation → database pipeline.

    Returns a summary dict for the Streamlit UI to display.
    """
    Path("data/logs").mkdir(parents=True, exist_ok=True)
    summary = {}
    t_start = time.time()

    log.info("=" * 60)
    log.info("MOTHERSON INVESTMENT INTELLIGENCE PIPELINE")
    log.info("=" * 60)

    # ── LAYER 1: Ingestion ───────────────────────────────────
    log.info("\n[Layer 1] Ingestion")
    t = time.time()
    df_raw = ingest(excel_path)
    save_raw_parquet(df_raw, "data/processed")
    summary["ingestion"] = {
        "rows":    len(df_raw),
        "cols":    len(df_raw.columns),
        "elapsed": round(time.time() - t, 2),
    }
    log.info("[Layer 1] Done — %d rows × %d cols (%.1fs)", len(df_raw), len(df_raw.columns), summary["ingestion"]["elapsed"])

    # ── LAYER 2: Cleaning ────────────────────────────────────
    log.info("\n[Layer 2] Cleaning")
    t = time.time()
    df_clean, log_file = clean(df_raw, log_path="data/logs/cleaning_log.csv")
    cleaning_summary   = get_cleaning_summary(df_raw, df_clean)
    summary["cleaning"] = {
        **cleaning_summary,
        "log_file": log_file,
        "elapsed":  round(time.time() - t, 2),
    }
    log.info("[Layer 2] Done — %d rows after cleaning (%.1fs)", len(df_clean), summary["cleaning"]["elapsed"])

    # ── LAYER 3: Validation ──────────────────────────────────
    log.info("\n[Layer 3] Validation")
    t = time.time()
    df_valid, df_quarantine, val_report = validate(
        df_clean,
        quarantine_path="data/logs/quarantine.csv",
        report_path="data/logs/validation_report.csv",
    )
    summary["validation"] = {
        **val_report,
        "elapsed": round(time.time() - t, 2),
    }
    log.info(
        "[Layer 3] Done — %d valid, %d quarantined, %.1f%% pass rate (%.1fs)",
        len(df_valid), len(df_quarantine),
        val_report["overall_pass_rate"], summary["validation"]["elapsed"],
    )

    # ── LAYER 4: Database ────────────────────────────────────
    log.info("\n[Layer 4] Database")
    t = time.time()
    engine = get_engine()
    # Note: create_schema() is called inside load_data() after DROP CASCADE.
    # Calling it here separately would fail if old tables exist with a different schema.
    db_counts = load_data(
        df_valid,
        df_quarantine,
        engine,
        if_exists="replace" if reload else "append",
    )
    db_stats = get_db_stats(engine)
    summary["database"] = {
        "tables_loaded":  db_counts,
        "db_row_counts":  db_stats,
        "elapsed":        round(time.time() - t, 2),
    }
    log.info("[Layer 4] Done (%.1fs)", summary["database"]["elapsed"])

    # ── Final summary ────────────────────────────────────────
    total_time = round(time.time() - t_start, 2)
    summary["total_elapsed"] = total_time
    summary["success"]       = True

    log.info("\n" + "=" * 60)
    log.info("PIPELINE COMPLETE in %.1fs", total_time)
    log.info("=" * 60)
    log.info("  Raw rows read   : %d", summary["ingestion"]["rows"])
    log.info("  Clean rows      : %d", summary["cleaning"]["rows_clean"])
    log.info("  Quarantined     : %d", summary["validation"]["quarantine_rows"])
    log.info("  DB investments  : %d", db_stats.get("investments", 0))
    log.info("  DB budget rows  : %d", db_stats.get("investment_budget", 0))
    log.info("  DB cashflow rows: %d", db_stats.get("investment_monthly_cashflow", 0))
    log.info("  Pass rate       : %.1f%%", summary["validation"]["overall_pass_rate"])
    log.info("=" * 60)

    return summary


# ── CLI ──────────────────────────────────────────────────────
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Motherson Investment Intelligence Pipeline")
    parser.add_argument("--excel",  default="data/raw/Tarun_-_Intern_Assignment-Data.xlsx", help="Path to Excel file")
    parser.add_argument("--reload", action="store_true", default=True, help="Drop and recreate DB tables")
    args = parser.parse_args()

    summary = run_pipeline(excel_path=args.excel, reload=args.reload)

    print("\n Pipeline complete!")
    print(f"   Total time : {summary['total_elapsed']}s")
    print(f"   Valid rows : {summary['validation']['valid_rows']:,}")
    print(f"   Pass rate  : {summary['validation']['overall_pass_rate']}%")
    print("\nYou can now run: streamlit run app.py")