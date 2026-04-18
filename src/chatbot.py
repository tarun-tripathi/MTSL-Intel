"""
Motherson Investment Intelligence — Chatbot v6

"""


import hashlib
import os
import re
from collections import deque

import pandas as pd
import requests
from sqlalchemy import text

# Optional: Gemini import (cloud only — local uses Ollama)
try:
    import google.generativeai as genai
    _GEMINI_SDK_AVAILABLE = True
except ImportError:
    _GEMINI_SDK_AVAILABLE = False

# ── CONSTANTS ──────────────────────────────────────────────
VALID_COMPANIES  = {"SMP", "SMRC", "MDRSC", "Other"}
VALID_REGIONS    = {
    "Germany & EE", "China", "LATAM", "Iberica",
    "France & North Africa", "Mexico", "USA"
}
VALID_CATEGORIES = {
    "Customer projects new", "Customer projects repeat",
    "Expansion", "Rationalization", "Replacement",
    "Environment, Health & Safety (EHS)", "Others", "Unknown"
}
KNOWN_CUSTOMERS = [
    "bmw", "mercedes", "volkswagen", "vw", "daimler", "audi",
    "ford", "toyota", "renault", "stellantis", "volvo", "hyundai",
    "kia", "porsche", "fca", "peugeot", "opel", "seat", "skoda",
    "jaguar", "land rover", "mini", "fiat", "chrysler", "jeep",
    "chery", "faw", "geely", "byd", "nio",
]
REGION_MAP = {
    "germany": "Germany & EE",   "deutschland": "Germany & EE",
    "china": "China",            "latam": "LATAM",
    "latin america": "LATAM",    "iberica": "Iberica",
    "spain": "Iberica",          "portugal": "Iberica",
    "mexico": "Mexico",          "usa": "USA",
    "america": "USA",            "france": "France & North Africa",
    "north africa": "France & North Africa",
    "africa": "France & North Africa",
    "hungary": "Germany & EE",   "poland": "Germany & EE",
    "czech": "Germany & EE",     "romania": "Germany & EE",
    "serbia": "Germany & EE",    "slovakia": "Germany & EE",
}
CATEGORY_MAP = {
    "ehs":             "Environment, Health & Safety (EHS)",
    "health":          "Environment, Health & Safety (EHS)",
    "safety":          "Environment, Health & Safety (EHS)",
    "environment":     "Environment, Health & Safety (EHS)",
    "expansion":       "Expansion",
    "rationalization": "Rationalization",
    "rationalisation": "Rationalization",
    "replacement":     "Replacement",
    "new customer":    "Customer projects new",
    "repeat customer": "Customer projects repeat",
}

# FY token → DB column mapping
FY_COL_MAP = {
    "this year":     "budget_fy_2526", "is saal":      "budget_fy_2526",
    "dieses jahr":   "budget_fy_2526", "aktuell":      "budget_fy_2526",
    "current":       "budget_fy_2526", "abhi":         "budget_fy_2526",
    "2526":          "budget_fy_2526", "2025":         "budget_fy_2526",
    "2025/26":       "budget_fy_2526", "fy2526":       "budget_fy_2526",
    "fy 2025":       "budget_fy_2526", "fy2025":       "budget_fy_2526",
    "fy 2025/26":    "budget_fy_2526",
    "next year":     "budget_fy_2627", "agla saal":    "budget_fy_2627",
    "nächstes":      "budget_fy_2627", "nächste":      "budget_fy_2627",
    "2627":          "budget_fy_2627", "2026":         "budget_fy_2627",
    "2026/27":       "budget_fy_2627", "fy2627":       "budget_fy_2627",
    "fy 2026":       "budget_fy_2627", "fy2026":       "budget_fy_2627",
    "fy 2026/27":    "budget_fy_2627",
    "going forward": "plan_fy_2728",   "forward":      "plan_fy_2728",
    "future":        "plan_fy_2728",   "aage":         "plan_fy_2728",
    "zukunft":       "plan_fy_2728",
    "2728":          "plan_fy_2728",   "2027":         "plan_fy_2728",
    "2027/28":       "plan_fy_2728",   "fy 2027":      "plan_fy_2728",
    "2829":          "plan_fy_2829",   "2028":         "plan_fy_2829",
    "2028/29":       "plan_fy_2829",   "fy 2028":      "plan_fy_2829",
    "2930":          "plan_fy_2930",   "2029":         "plan_fy_2930",
    "2029/30":       "plan_fy_2930",   "fy 2029":      "plan_fy_2930",
    # 5-year signals
    "5y":            "total_5y_k_eur", "5year":        "total_5y_k_eur",
    "5 year":        "total_5y_k_eur", "five year":    "total_5y_k_eur",
    "all year":      "total_5y_k_eur", "all years":    "total_5y_k_eur",
    "across all":    "total_5y_k_eur", "whole":        "total_5y_k_eur",
    "full 5":        "total_5y_k_eur", "poora":        "total_5y_k_eur",
    "panch saal":    "total_5y_k_eur", "5-year":       "total_5y_k_eur",
    "full plan":     "total_5y_k_eur",
}

# NEW-5: True 5-year total = sum of all 5 annual budget columns
TRUE_5Y_SQL = """
    SELECT
        SUM(budget_fy_2526 + budget_fy_2627 + plan_fy_2728 + plan_fy_2829 + plan_fy_2930)
        AS total_5y_k_eur
    FROM investment_budget
"""

BUDGET_COLS = {
    "budget_fy_2526","budget_fy_2627","plan_fy_2728",
    "plan_fy_2829","plan_fy_2930","total_5y_k_eur",
    "budget_k_eur","total_k_eur","amount_k_eur",
    "avg_budget_k_eur","budget_eur_2526","budget_eur_2627",
}

# NEW-2: Intents that are FINANCIAL → always ask year before answering
FINANCIAL_INTENTS = {
    "by_company_budget", "by_region_budget", "by_plant",
    "by_customer_budget", "top_n", "threshold", "zero_budget",
    "compare", "capex",
}

# Intents that are STRUCTURAL → never ask year, use count/5y
STRUCTURAL_INTENTS = {
    "count_total", "count_by", "list_detail",
    "by_company_count", "by_region_count", "average",
    "cashflow", "tangible_split", "by_category",
}


# ── UTILITY ────────────────────────────────────────────────

def format_large_number(value_k) -> str:
    try:
        v = float(value_k) * 1000
    except Exception:
        return str(value_k)
    if abs(v) >= 1_000_000_000:
        return f"€ {v/1_000_000_000:.2f}B"
    elif abs(v) >= 1_000_000:
        return f"€ {v/1_000_000:.2f}M"
    elif abs(v) >= 1_000:
        n = v / 1_000
        return f"€ {n:.0f}K" if n == int(n) else f"€ {n:.1f}K"
    return f"€ {v:,.0f}"

def fmt(v) -> str:
    return format_large_number(v)

# Columns that are pure counts — must NEVER be formatted as currency
_COUNT_COLS = {
    "count", "investment_count", "total_investments", "project_count",
    "plant_count", "region_count", "company_count",
}

def format_df_for_display(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    out = df.copy()
    for col in out.columns:
        col_l = col.lower()
        # Skip pure count columns — they are integers, not currency values
        if col_l in _COUNT_COLS or col_l.endswith("_count"):
            continue
        if col_l in BUDGET_COLS or any(
            x in col_l for x in ["budget","plan","total","amount","_eur","avg_"]
        ):
            if pd.to_numeric(out[col], errors="coerce").notna().any():
                out[col] = out[col].apply(
                    lambda x: format_large_number(x) if pd.notna(x) else ""
                )
    return out

def _fy_label(fc: str, lang: str) -> str:
    labels = {
        "budget_fy_2526": "FY 2025/26",
        "budget_fy_2627": "FY 2026/27",
        "plan_fy_2728":   "FY 2027/28",
        "plan_fy_2829":   "FY 2028/29",
        "plan_fy_2930":   "FY 2029/30",
        "total_5y_k_eur": {
            "english":  "5-year total (2025–2030)",
            "hindi":    "5 साल का कुल (2025–2030)",
            "hinglish": "5 saal ka total (2025–2030)",
            "german":   "5-Jahres-Gesamt (2025–2030)",
        },
    }
    val = labels.get(fc, fc)
    if isinstance(val, dict):
        return val.get(lang, val.get("english", "5-year total"))
    return val


# ── LANGUAGE DETECTION (per message — NEW-1) ───────────────

HINDI_RE     = re.compile(r'[\u0900-\u097F]')
GERMAN_WORDS = {
    # Only words that are EXCLUSIVELY German — never appear in English
    "was","ist","wie","viel","gesamt","unternehmen","investition",
    "welche","welcher","nächste","nächstes","dieses","wieviel",
    "zeige","zeig","höchste","niedrigste","anzahl","zukunft",
    "firma","gibt","projekte","viele","vergleich","zwischen",
    "gesamtbudget","investitionen","bitte","zeigen","alle",
    "welches","wieviele","geschäftsjahr","laufendes","nächstes",
    # Removed: "budget","plan","region","nach","mir" — these are also English
}
HINGLISH_WORDS = {
    "kitna","kitne","batao","bata","karo","kya","hai","hain",
    "ka","ke","ki","mein","se","aur","nahi","sab","dikhao",
    "bhai","yaar","ginti","sabse","zyada","saal","paisa",
    "dikha","compare","kaun","konsa","poora","kul","lagao",
}

def detect_lang(text: str) -> str:
    """NEW-1: detect language of THIS message — not locked to session."""
    if HINDI_RE.search(text):        return "hindi"
    words = set(text.lower().split())
    if words & GERMAN_WORDS:         return "german"
    if words & HINGLISH_WORDS:       return "hinglish"
    return "english"


# ── KEYWORD NORMALISER ─────────────────────────────────────

PHRASE_MAP = {
    "agla saal":           "next year",
    "is saal":             "this year",
    "wie viele":           "how many",
    "wie viel":            "total",
    "north africa":        "north africa",
    "land rover":          "land rover",
    "latin america":       "latam",
    "going forward":       "going forward",
    "capital expenditure": "capex",
    "all years":           "all years",
    "across all years":    "all years",
    "full 5-year":         "full 5 year",
    "full 5 year":         "full 5 year",
    "5-year plan":         "full 5 year",
    "fy 2025/26":          "2025/26",
    "fy 2026/27":          "2026/27",
    "fy 2027/28":          "2027/28",
    "fy 2028/29":          "2028/29",
    "fy 2029/30":          "2029/30",
    "fy2025/26":           "2025/26",
    "fy2026/27":           "2026/27",
    "zero budget":         "zero budget",
    "no budget":           "zero budget",
    "null budget":         "zero budget",
}
WORD_MAP = {
    "kitna":"total",     "kitne":"count",    "kul":"total",
    "batao":"show",      "dikhao":"show",    "dikha":"show",
    "sabse":"top",       "zyada":"highest",  "jyada":"highest",
    "saal":"year",       "agla":"next",      "paisa":"budget",
    "nivesh":"investment","ginti":"count",
    "gesamt":"total",    "unternehmen":"company","nach":"by",
    "firma":"company",   "anzahl":"count",   "viele":"count",
    "zukunft":"future",  "nächste":"next",   "dieses":"this",
    "zeige":"show",      "zwischen":"between","vergleich":"compare",
    "kaun":"which",      "konsa":"which",
    "compare":"compare", "vs":"vs",          "versus":"vs",
    "capex":"capex",     "capital":"capex",
    "forward":"going forward","future":"going forward","aage":"going forward",
    "spent":"actual",    "disbursed":"actual","utilized":"actual",
    "average":"avg",     "avg":"avg",        "mean":"avg",
    "durchschnitt":"avg",
    "gesamtbudget":"total budget",
    "investitionen":"investments",
    "zeig":"show",       "mir":"me",         "alle":"all",
    "null":"zero",       "keine":"no",
}

def normalise(text: str) -> str:
    t = text.lower().strip()
    for src, tgt in PHRASE_MAP.items():
        t = t.replace(src, tgt)
    for src, tgt in WORD_MAP.items():
        t = re.sub(r'\b' + re.escape(src) + r'\b', tgt, t)
    return t


# ── MULTILINGUAL REPLY TEMPLATES ───────────────────────────

def R(key: str, lang: str, **kwargs) -> str:
    templates = {
        "out_of_scope": {
            "english":  "I only answer questions about **Motherson investment data** — budgets, regions, companies, plants, customers and cashflows.",
            "hindi":    "मैं केवल **Motherson निवेश डेटा** के बारे में उत्तर दे सकता हूँ।",
            "hinglish": "Bhai, main sirf **Motherson investment data** ke baare mein bata sakta hoon!",
            "german":   "Ich beantworte nur Fragen zu **Motherson-Investitionsdaten**.",
        },
        # NEW-2: Full year clarification with all 5 years + 5-year option
        "clarify_year": {
            "english":  (
                "Which fiscal year?\n"
                "- **FY 2025/26** — current year\n"
                "- **FY 2026/27** — next year\n"
                "- **FY 2027/28**\n"
                "- **FY 2028/29**\n"
                "- **FY 2029/30**\n"
                "- **Full 5-year total** (2025–2030)"
            ),
            "hindi":  (
                "कौन सा वित्त वर्ष?\n"
                "- **FY 2025/26** (चालू वर्ष)\n"
                "- **FY 2026/27** (अगला वर्ष)\n"
                "- **FY 2027/28**\n"
                "- **FY 2028/29**\n"
                "- **FY 2029/30**\n"
                "- **पूरे 5 साल का कुल** (2025–2030)"
            ),
            "hinglish": (
                "Kaun sa year bhai?\n"
                "- **FY 2025/26** (is saal)\n"
                "- **FY 2026/27** (agla saal)\n"
                "- **FY 2027/28**\n"
                "- **FY 2028/29**\n"
                "- **FY 2029/30**\n"
                "- **Poore 5 saal ka total** (2025–2030)"
            ),
            "german": (
                "Welches Geschäftsjahr?\n"
                "- **GJ 2025/26** — laufendes Jahr\n"
                "- **GJ 2026/27** — nächstes Jahr\n"
                "- **GJ 2027/28**\n"
                "- **GJ 2028/29**\n"
                "- **GJ 2029/30**\n"
                "- **Gesamter 5-Jahres-Plan** (2025–2030)"
            ),
        },
        "clarify_vague_total": {
            "english":  "Total of what?\n- **Budget** (pick a fiscal year)\n- **Count** of investments\n- **By region** or by company?\n\nExample: _'What is the total budget for FY 2025/26?'_",
            "hindi":    "कुल किसका? बजट (वर्ष चुनें), गिनती, या क्षेत्र/कंपनी द्वारा?",
            "hinglish": "Total kiska bhai? Budget (year batao), count, ya region/company ke hisaab se?",
            "german":   "Gesamt von was? Budget (Jahr wählen), Anzahl oder nach Region/Unternehmen?",
        },
        "clarify_vague_show": {
            "english":  "I can show investments filtered by:\n- **Region** (e.g. Germany, China)\n- **Company** (SMP, SMRC, MDRSC)\n- **Category** (Expansion, Replacement, EHS…)\n- **Customer** (BMW, Mercedes…)\n\nWhat would you like to filter by?",
            "hindi":    "निवेश फ़िल्टर करने के लिए बताएं — क्षेत्र, कंपनी, श्रेणी, या ग्राहक?",
            "hinglish": "Kaunse investments dikhaaun? Region, company, category ya customer — kuch batao!",
            "german":   "Nach was filtern — Region, Unternehmen, Kategorie oder Kunde?",
        },
        "no_actuals": {
            "english":  "**Actual spend data is not available** at the investment level.\n\nWhat IS available:\n- **Quarterly budgets**: Q2/Q3/Q4 FY2025 and Q1–Q4 FY2026\n- **Monthly cashflow**: try _'Show me monthly cashflow for FY 2025/26'_",
            "hindi":    "**वास्तविक व्यय डेटा उपलब्ध नहीं है।** त्रैमासिक बजट या मासिक कैशफ्लो उपलब्ध है।",
            "hinglish": "Actual spend data nahi hai bhai. Quarterly budget ya monthly cashflow dekh sakte hain!",
            "german":   "**Ist-Ausgaben sind nicht verfügbar.** Quartalsdaten und monatlicher Cashflow sind vorhanden.",
        },
        "no_status": {
            "english":  "**Investment status is not tracked** in this database.\n\nYou can filter by company, region, plant, category, or customer instead.",
            "hindi":    "**निवेश की स्थिति उपलब्ध नहीं है।** क्षेत्र, कंपनी या श्रेणी द्वारा फ़िल्टर करें।",
            "hinglish": "Status ka data nahi hai. Region, company ya category filter try karo!",
            "german":   "**Investitionsstatus ist nicht verfügbar.** Filtern nach Region, Unternehmen oder Kategorie möglich.",
        },
        "no_data": {
            "english":  "No data found. Try adding a company, region, or fiscal year.",
            "hindi":    "कोई डेटा नहीं मिला। कंपनी, क्षेत्र या वर्ष जोड़ें।",
            "hinglish": "Koi data nahi mila. Company, region ya year add karke try karo.",
            "german":   "Keine Daten gefunden. Bitte Unternehmen, Region oder Jahr angeben.",
        },
        "sql_error": {
            "english":  "Database error — try rephrasing your question.",
            "hindi":    "डेटाबेस त्रुटि। प्रश्न दोबारा पूछें।",
            "hinglish": "Database error aa gayi. Alag tarike se pucho.",
            "german":   "Datenbankfehler. Bitte anders formulieren.",
        },
        "unsafe": {
            "english":  "That query is not allowed.",
            "hindi":    "यह क्वेरी अनुमत नहीं है।",
            "hinglish": "Yeh query allowed nahi hai.",
            "german":   "Diese Abfrage ist nicht erlaubt.",
        },
        "internal_error": {
            "english":  "An internal error occurred. Please try again.",
            "hindi":    "आंतरिक त्रुटि हुई। पुनः प्रयास करें।",
            "hinglish": "Kuch error aa gayi. Dobara try karo.",
            "german":   "Interner Fehler. Bitte erneut versuchen.",
        },
    }
    t = templates.get(key, {}).get(lang) or templates.get(key, {}).get("english", key)
    for k, v in kwargs.items():
        t = t.replace("{" + k + "}", str(v))
    return t


# ── OUT-OF-SCOPE ───────────────────────────────────────────

OOS_PATTERNS = [
    r'\bwhat are you\b', r'\bwho are you\b', r'\btum kaun ho\b',
    r'\bwas bist du\b',  r'\bwer bist du\b',
    r'\bhow are you\b',  r'\bkaise ho\b',
    r'\bjoke\b',         r'\bweather\b',    r'\bwetter\b',
    r'\brecipe\b',       r'\bcook\b',       r'\bkhana\b',
    r'\bstock price\b',  r'\bnews\b',       r'\bpoem\b',
    r'\btranslate\b',    r'\bwrite (a|an) (story|essay|letter)\b',
]
def is_oos(q: str) -> bool:
    return any(re.search(p, q) for p in OOS_PATTERNS)


# Year detection regex — does query already specify a year?
YEAR_PRESENT_RE = re.compile(
    r'(this year|next year|is saal|agla saal|dieses jahr|current|going forward|future|'
    r'all years?|across all|full 5|five year|5.year|5y|panch saal|poora|whole|'
    r'2025/26|2026/27|2027/28|2028/29|2029/30|'
    r'fy\s*20\d\d|fy\s*2[05][2-9][0-9]|'
    r'\b202[5-9]\b|\b203[0-9]\b|'
    r'\b2526\b|\b2627\b|\b2728\b|\b2829\b|\b2930\b)',
    re.I
)


# ── SAFE SQL ───────────────────────────────────────────────

BLOCKED_KW = ["DROP","DELETE","UPDATE","INSERT","ALTER","TRUNCATE","EXEC","GRANT","REVOKE"]

def is_safe(sql: str) -> bool:
    if not sql: return False
    s = sql.strip().upper()
    return s.startswith("SELECT") and not any(b in s for b in BLOCKED_KW)

def sanitise(sql: str) -> str:
    if not sql: return sql
    sql = re.sub(r'\bfiscal_year\s*=\s*[\'"]?20\d\d/?2\d[\'"]?', "1=1", sql, flags=re.I)
    sql = re.sub(r'\binvestments_budget\b', "investment_budget", sql, flags=re.I)
    sql = re.sub(
        r'\bFROM\s+(customers|projects|budget)\b',
        lambda m: "FROM investment_budget" if m.group(1)=="budget" else "FROM investments",
        sql, flags=re.I
    )
    if ";" in sql: sql = sql.split(";")[0].strip()
    return sql


# ── LLM SCHEMA PROMPT ──────────────────────────────────────

SCHEMA = """
You are an expert SQL writer for the Motherson Investment Intelligence PostgreSQL database.
The user may ask in English, Hindi, Hinglish, or German.
If the question is in German, translate it mentally to understand the intent, then write SQL.
Always return ONLY raw SQL — no explanation, no markdown, no German text.

=== EXACT SCHEMA (never invent columns or tables) ===

TABLE investments  (5,172 rows)
  investment_id   INTEGER PRIMARY KEY
  company         TEXT    -- values: 'SMP', 'SMRC', 'MDRSC', 'Other'
  region          TEXT    -- values: 'Germany & EE', 'China', 'LATAM', 'Iberica',
                          --         'France & North Africa', 'Mexico', 'USA'
  plant           TEXT    -- e.g. 'CEFA Poland', 'SMRC Nitra', 'SMP Serbia'
  investment_description  TEXT
  investment_category     TEXT  -- values: 'Customer projects new',
                                --   'Customer projects repeat', 'Expansion',
                                --   'Rationalization', 'Replacement',
                                --   'Environment, Health & Safety (EHS)',
                                --   'Others', 'Unknown'
  customer        TEXT    -- e.g. 'BMW', 'Daimler', 'Volkswagen', 'Unknown'
  car_model       TEXT
  source_of_funding TEXT  -- values: 'Own', 'Leasing', 'Borrowings', 'Customer', 'Other'
  tangible_intangible TEXT  -- values: 'Tangible', 'Intangible'
  productive_non_productive TEXT  -- values: 'Productive', 'Non Productive', 'Unknown'
  mpp_value_level TEXT    -- values: '>=1m', '>=500k & <1m', '>=200k & <500k', '<200k'
  local_currency  TEXT

TABLE investment_budget  (one row per investment)
  row_id          INTEGER  (UNIQUE, FK → investments.row_id)
  -- Annual budgets in k EUR (thousands of euros):
  budget_fy_2526  NUMERIC  -- FY 2025/26 = "this year" / "aktuelles Jahr" / "is saal"
  budget_fy_2627  NUMERIC  -- FY 2026/27 = "next year"  / "nächstes Jahr"  / "agla saal"
  plan_fy_2728    NUMERIC  -- FY 2027/28
  plan_fy_2829    NUMERIC  -- FY 2028/29
  plan_fy_2930    NUMERIC  -- FY 2029/30
  total_5y_k_eur  NUMERIC  -- pre-calculated 5-year total
  budget_eur_2526, budget_eur_2627, plan_eur_2728, plan_eur_2829, plan_eur_2930, total_eur_5y
  -- Quarterly: budget_q2_2025, budget_q3_2025, budget_q4_2025, budget_q1_2026,
  --            budget_q2_2026, budget_q3_2026, budget_q4_2026, budget_q1_2027

TABLE investment_monthly_cashflow
  row_id          INTEGER  (FK → investments.row_id)
  period_date     DATE     -- e.g. '2025-04-01'
  fiscal_year     TEXT     -- '2025/26' or '2026/27'
  amount_k_eur    NUMERIC

=== STRICT RULES ===
1. All budget values are in k EUR (thousands). SUM(budget_fy_2526) = total in k EUR.
2. Always JOIN investments i JOIN investment_budget b ON i.row_id = b.row_id
3. COUNT(*) for counting investments — never SUM for a count
4. Use ILIKE for text search: LOWER(i.plant) ILIKE '%poland%'
5. Only SELECT statements — never INSERT/UPDATE/DELETE/DROP/ALTER/CREATE
6. Return ONLY the raw SQL query — no explanation, no markdown fences, no commentary
7. No "actual spend", "realized", "disbursed" column exists — do not invent one
8. No "status", "approved", "started", "completed" column exists — do not invent one
9. If you cannot write a valid query, return exactly: SELECT 'unsupported' AS error

=== FEW-SHOT EXAMPLES ===

Q: What is the total budget for SMP in FY 2025/26?
SQL: SELECT SUM(b.budget_fy_2526) AS total_k_eur FROM investments i JOIN investment_budget b ON i.row_id = b.row_id WHERE i.company = 'SMP'

Q: How many investments are there in China?
SQL: SELECT COUNT(*) AS investment_count FROM investments WHERE region = 'China'

Q: Show top 5 plants by budget this year
SQL: SELECT i.plant, SUM(b.budget_fy_2526) AS budget_k_eur FROM investments i JOIN investment_budget b ON i.row_id = b.row_id GROUP BY i.plant ORDER BY budget_k_eur DESC LIMIT 5

Q: Wie hoch ist das Gesamtbudget für die Region Deutschland?
SQL: SELECT SUM(b.budget_fy_2526) AS total_k_eur FROM investments i JOIN investment_budget b ON i.row_id = b.row_id WHERE i.region = 'Germany & EE'

Q: SMP ka total 5 saal ka budget kitna hai?
SQL: SELECT SUM(b.budget_fy_2526 + b.budget_fy_2627 + b.plan_fy_2728 + b.plan_fy_2829 + b.plan_fy_2930) AS total_5y_k_eur FROM investments i JOIN investment_budget b ON i.row_id = b.row_id WHERE i.company = 'SMP'

Q: Which investment categories exist and how many projects each?
SQL: SELECT investment_category, COUNT(*) AS project_count FROM investments GROUP BY investment_category ORDER BY project_count DESC

Q: Wie viele Investitionen hat SMRC insgesamt?
SQL: SELECT COUNT(*) AS investment_count FROM investments WHERE company = 'SMRC'
"""


# ── INTENT PARSER ──────────────────────────────────────────

def parse_intent(q_norm: str, q_raw: str) -> dict:
    intent = {
        "company":         None,
        "region":          None,
        "region2":         None,   # second region for compare queries
        "customer":        None,
        "category":        None,
        "plant_search":    None,
        "fy_col":          None,
        "fy_col2":         None,
        "compare":         False,
        "top_n":           None,
        "threshold":       None,
        "zero_budget":     False,
        "intent_type":     "general",
        "requested_chart": None,
    }

    # Company
    for co in ["smp","smrc","mdrsc"]:
        if re.search(r'\b' + co + r'\b', q_raw, re.I):
            intent["company"] = co.upper()
            break

    # Region
    for kw, rv in REGION_MAP.items():
        if re.search(r'\b' + re.escape(kw) + r'\b', q_raw, re.I):
            intent["region"] = rv
            break

    # NEW-3: plant-level search — detect plant keywords not covered by region map
    # e.g. "SMP Serbia" → company=SMP, plant_search=serbia
    plant_keywords = [
        "serbia","neustadt","oldenburg","gottingen","kaluga","nitra",
        "cpat","gondecourt","palmela","zitlaltepec","tetouan","boetzingen",
        "shenyang","tianjin","radomierz","neuses","hungary","poland",
    ]
    for pk in plant_keywords:
        if re.search(r'\b' + pk + r'\b', q_raw, re.I):
            intent["plant_search"] = pk
            break

    # Customer
    for name in KNOWN_CUSTOMERS:
        if re.search(r'\b' + re.escape(name) + r'\b', q_raw, re.I):
            intent["customer"] = name
            break

    # Category
    for kw, cv in CATEGORY_MAP.items():
        if kw in q_norm:
            intent["category"] = cv
            break

    # NEW-4: zero budget detection
    if re.search(r'\b(zero budget|no budget|null budget|zero.{0,10}budget|budget.{0,10}zero)\b', q_norm):
        intent["zero_budget"] = True

    # Fiscal year — scan FY_COL_MAP tokens
    for token, col in FY_COL_MAP.items():
        if token in q_norm:
            intent["fy_col"] = col
            break
    # Raw scan for slash formats
    if re.search(r'2025/26|fy\s*2025\b', q_raw, re.I):
        intent["fy_col"] = "budget_fy_2526"
    elif re.search(r'2026/27|fy\s*2026\b', q_raw, re.I):
        intent["fy_col"] = "budget_fy_2627"
    elif re.search(r'2027/28|fy\s*2027\b', q_raw, re.I):
        intent["fy_col"] = "plan_fy_2728"
    elif re.search(r'2028/29|fy\s*2028\b', q_raw, re.I):
        intent["fy_col"] = "plan_fy_2829"
    elif re.search(r'2029/30|fy\s*2029\b', q_raw, re.I):
        intent["fy_col"] = "plan_fy_2930"

    # Explicit chart type
    if re.search(r'\bbar chart\b', q_norm):    intent["requested_chart"] = "bar"
    elif re.search(r'\bpie chart\b', q_norm):  intent["requested_chart"] = "pie"
    elif re.search(r'\bline chart\b', q_norm): intent["requested_chart"] = "line"

    # Comparison
    if re.search(r'\b(vs|versus|compare|between|against|vergleich|zwischen)\b', q_norm):
        intent["compare"] = True
        fy_found = []
        for token, col in FY_COL_MAP.items():
            if token in q_norm and col not in fy_found:
                fy_found.append(col)
        for pat, col in [
            (r'2025/26', "budget_fy_2526"),
            (r'2026/27', "budget_fy_2627"),
            (r'2027/28', "plan_fy_2728"),
        ]:
            if re.search(pat, q_raw) and col not in fy_found:
                fy_found.append(col)
        if len(fy_found) >= 2:
            intent["fy_col"]  = fy_found[0]
            intent["fy_col2"] = fy_found[1]

        # Detect second region for region vs region comparison
        if intent["region"]:
            regions_found = []
            for key, val in REGION_MAP.items():
                if key in q_raw.lower():
                    if val not in regions_found:
                        regions_found.append(val)
            if len(regions_found) >= 2:
                intent["region"]  = regions_found[0]
                intent["region2"] = regions_found[1]

    # Top-N
    top_m = re.search(r'\btop\s+(\d+)\b', q_norm)
    if top_m:
        intent["top_n"] = int(top_m.group(1))

    # Threshold
    thresh_m = re.search(
        r'(?:above|over|more than|greater than|>)\s*([\d,.]+)\s*(m|million|k|thousand|b|billion)?',
        q_norm
    )
    if thresh_m:
        val = float(thresh_m.group(1).replace(",",""))
        unit = (thresh_m.group(2) or "").lower()
        if unit in ("m","million"):   val *= 1000
        elif unit in ("b","billion"): val *= 1_000_000
        intent["threshold"] = val

    # ── Intent type ────────────────────────────────────────
    if re.search(r'\b(capex)\b', q_norm):
        intent["intent_type"] = "capex"
    elif intent["zero_budget"]:
        intent["intent_type"] = "zero_budget"
    elif intent["compare"]:
        intent["intent_type"] = "compare"
    elif intent["top_n"]:
        intent["intent_type"] = "top_n"
    elif intent["threshold"]:
        intent["intent_type"] = "threshold"
    # NEW-2: split company/region intent into budget vs count
    elif re.search(r'\b(per company|how many.{0,20}compan|count.{0,20}compan|compan.{0,20}count)\b', q_norm):
        intent["intent_type"] = "by_company_count"
    elif re.search(r'\b(per region|how many.{0,20}region|count.{0,20}region)\b', q_norm):
        intent["intent_type"] = "by_region_count"
    elif re.search(r'\b(budget.{0,20}compan|compan.{0,20}budget|by company|company.wise|total.{0,10}compan)\b', q_norm) or \
         (not intent["company"] and re.search(r'\bcompan\b', q_norm) and re.search(r'\b(budget|total|spend)\b', q_norm)):
        intent["intent_type"] = "by_company_budget"
    elif re.search(r'\b(budget.{0,20}region|region.{0,20}budget|by region|region.wise)\b', q_norm) or \
         (not intent["region"] and re.search(r'\bregion\b', q_norm) and re.search(r'\b(budget|total|spend)\b', q_norm)):
        intent["intent_type"] = "by_region_budget"
    elif re.search(r'\b(by plant|plant.wise|top plant|highest.{0,20}plant|plant.{0,20}highest|which plant)\b', q_norm):
        intent["intent_type"] = "by_plant"
    elif re.search(r'\b(by category|categor|investment categor)\b', q_norm) or \
         (intent["requested_chart"] == "pie" and re.search(r'categor', q_norm)):
        intent["intent_type"] = "by_category"
    elif re.search(r'\b(by customer|customer.wise|top customer)\b', q_norm):
        intent["intent_type"] = "by_customer_budget"
    elif re.search(r'\b(monthly|cashflow|month.wise)\b', q_norm):
        intent["intent_type"] = "cashflow"
    elif re.search(r'\b(tangible|intangible)\b', q_norm):
        intent["intent_type"] = "tangible_split"
    elif re.search(r'\bavg\b', q_norm):
        intent["intent_type"] = "average"
    # Structural counts
    elif re.search(r'\b(how many|count|kitne|anzahl|ginti)\b', q_norm) and \
         not re.search(r'\b(budget|eur|spend)\b', q_norm):
        # Special case: count distinct plants/regions/companies
        if re.search(r'\bplant', q_norm):
            intent["intent_type"] = "count_plants"
        elif re.search(r'\bregion', q_norm) and not co and not intent["region"]:
            intent["intent_type"] = "count_regions"
        elif re.search(r'\bcompan', q_norm) and not co:
            intent["intent_type"] = "by_company_count"
        else:
            intent["intent_type"] = "count_total"
    # List detail — only when a filter is present
    elif re.search(r'\b(show|list|display|give me)\b', q_norm) and \
         re.search(r'\b(all\s+)?(investments?|projects?)\b', q_norm) and \
         (intent["region"] or intent["company"] or intent["customer"] or intent["category"]):
        intent["intent_type"] = "list_detail"

    return intent


# ── NEEDS YEAR? ────────────────────────────────────────────

def needs_year(intent: dict) -> bool:
    """
    NEW-2: Returns True if this intent is FINANCIAL and needs year clarification.
    Returns False if STRUCTURAL (use count/overall).
    """
    itype = intent["intent_type"]
    # Already has year → no clarification needed
    if intent["fy_col"] is not None:
        return False
    # Structural intents → never need year
    if itype in STRUCTURAL_INTENTS:
        return False
    # Financial intents → need year
    financial = {
        "by_company_budget","by_region_budget","by_plant","by_customer_budget",
        "top_n","threshold","zero_budget","compare","capex","general",
    }
    return itype in financial


# ── SQL BUILDER ────────────────────────────────────────────

# Whitelist of allowed fiscal-year column names (safe to interpolate as identifiers)
_ALLOWED_FY_COLS = {
    "budget_fy_2526", "budget_fy_2627", "plan_fy_2728",
    "plan_fy_2829",   "plan_fy_2930",   "total_5y_k_eur",
}

def build_sql(intent: dict, q_norm: str, q_raw: str) -> tuple[str, dict] | None:
    """
    Returns (sql_string, params_dict) or None.
    All user-supplied filter values use SQLAlchemy bound parameters (:name)
    so they are never interpolated into the query string.
    Column names (fiscal-year columns) are validated against a whitelist
    before interpolation — they are identifiers, not values.
    """
    # Validate and resolve fiscal-year column names (identifiers, not values)
    raw_fc  = intent["fy_col"] or "budget_fy_2526"
    raw_fc2 = intent["fy_col2"]
    fc  = raw_fc  if raw_fc  in _ALLOWED_FY_COLS else "budget_fy_2526"
    fc2 = raw_fc2 if raw_fc2 in _ALLOWED_FY_COLS else None

    co    = intent["company"]
    reg   = intent["region"]
    cust  = intent["customer"]
    cat   = intent["category"]
    ps    = intent.get("plant_search")
    n     = intent["top_n"]
    thr   = intent["threshold"]
    itype = intent["intent_type"]

    def where(extras=None):
        """
        Returns (where_clause_str, params_dict).
        User values → bound params.  extras → safe hardcoded strings only.
        """
        conds  = []
        params = {}
        if co:
            conds.append("i.company = :company")
            params["company"] = co
        if reg:
            conds.append("i.region = :region")
            params["region"] = reg
        if ps:
            conds.append("LOWER(i.plant) ILIKE :plant_search")
            params["plant_search"] = f"%{ps.lower()}%"
        if cust:
            conds.append("LOWER(i.customer) LIKE :customer")
            params["customer"] = f"%{cust.lower()}%"
        if cat:
            conds.append("i.investment_category = :category")
            params["category"] = cat
        if extras:
            conds.extend(extras)   # extras are hardcoded safe strings, not user input
        clause = ("WHERE " + " AND ".join(conds)) if conds else ""
        return clause, params

    join = "FROM investments i JOIN investment_budget b ON i.row_id = b.row_id"
    lim  = f"LIMIT {n}" if n else ""

    # NEW-4: zero budget
    if itype == "zero_budget":
        w, p = where([f"b.{fc} = 0"])
        return f"""
            SELECT i.investment_id, i.company, i.region, i.plant,
                   i.investment_category, i.customer, b.{fc} AS budget_k_eur
            {join} {w}
            ORDER BY i.company, i.region LIMIT 100
        """, p

    # Capex
    if itype == "capex":
        w, p = where(["i.tangible_intangible = 'Tangible'",
                      "i.productive_non_productive = 'Productive'"])
        return f"""
            SELECT i.company, i.region, i.investment_category,
                   COUNT(*) AS project_count, SUM(b.{fc}) AS budget_k_eur
            {join} {w}
            GROUP BY i.company, i.region, i.investment_category
            ORDER BY budget_k_eur DESC {lim or 'LIMIT 50'}
        """, p

    # Structural: distinct entity counts
    if itype == "count_plants":
        return "SELECT COUNT(DISTINCT plant) AS plant_count FROM investments", {}

    if itype == "count_regions":
        return "SELECT COUNT(DISTINCT region) AS region_count FROM investments", {}

    # Structural: total count
    if itype == "count_total":
        if co or reg or cust or cat:   # FIX: added cat
            w, p = where()
            return f"SELECT COUNT(*) AS investment_count FROM investments i {w}", p
        return "SELECT COUNT(*) AS total_investments FROM investments", {}

    # Structural: count by company
    if itype == "by_company_count":
        return "SELECT company, COUNT(*) AS investment_count FROM investments GROUP BY company ORDER BY investment_count DESC", {}

    # Structural: count by region
    if itype == "by_region_count":
        return "SELECT region, COUNT(*) AS investment_count FROM investments GROUP BY region ORDER BY investment_count DESC", {}

    # Structural: average
    if itype == "average":
        if re.search(r'region', q_norm):
            return f"""
                SELECT i.region,
                       ROUND(AVG(b.{fc})::numeric, 2) AS avg_budget_k_eur,
                       COUNT(*) AS project_count
                {join} GROUP BY i.region ORDER BY avg_budget_k_eur DESC
            """, {}
        if re.search(r'compan', q_norm):
            return f"""
                SELECT i.company,
                       ROUND(AVG(b.{fc})::numeric, 2) AS avg_budget_k_eur,
                       COUNT(*) AS project_count
                {join} GROUP BY i.company ORDER BY avg_budget_k_eur DESC
            """, {}
        return f"SELECT ROUND(AVG({fc})::numeric, 2) AS avg_budget_k_eur FROM investment_budget", {}

    # List detail
    if itype == "list_detail":
        w, p = where()
        return f"""
            SELECT i.investment_id, i.company, i.region, i.plant,
                   i.investment_category, i.customer,
                   b.{fc} AS budget_k_eur, b.total_5y_k_eur
            {join} {w}
            ORDER BY b.{fc} DESC NULLS LAST LIMIT 50
        """, p

    # Compare: region vs region
    reg2 = intent.get("region2")
    if itype == "compare" and reg and reg2:
        return f"""
            SELECT :reg1 AS region, SUM(b.{fc}) AS budget_k_eur, COUNT(*) AS project_count
            FROM investments i JOIN investment_budget b ON i.row_id = b.row_id
            WHERE i.region = :reg1
            UNION ALL
            SELECT :reg2, SUM(b.{fc}), COUNT(*)
            FROM investments i JOIN investment_budget b ON i.row_id = b.row_id
            WHERE i.region = :reg2
        """, {"reg1": reg, "reg2": reg2}

    # Compare: FY vs FY
    if itype == "compare" and fc2:
        w, p = where()
        return f"""
            SELECT '{fc}' AS fiscal_year, SUM(b.{fc}) AS budget_k_eur
            FROM investments i JOIN investment_budget b ON i.row_id = b.row_id {w}
            UNION ALL
            SELECT '{fc2}', SUM(b.{fc2})
            FROM investments i JOIN investment_budget b ON i.row_id = b.row_id {w}
        """, p

    # Compare: two customers (from KNOWN_CUSTOMERS whitelist — safe to interpolate labels)
    if itype == "compare" and cust:
        first, second = cust, None
        for name in KNOWN_CUSTOMERS:
            if name != first and re.search(r'\b'+re.escape(name)+r'\b', q_raw, re.I):
                second = name; break
        if second:
            return f"""
                SELECT '{first.upper()}' AS customer,
                    SUM(CASE WHEN LOWER(i.customer) LIKE :cust1 THEN b.{fc} ELSE 0 END) AS budget_k_eur,
                    COUNT(CASE WHEN LOWER(i.customer) LIKE :cust1 THEN 1 END) AS project_count
                FROM investments i JOIN investment_budget b ON i.row_id = b.row_id
                UNION ALL
                SELECT '{second.upper()}',
                    SUM(CASE WHEN LOWER(i.customer) LIKE :cust2 THEN b.{fc} ELSE 0 END),
                    COUNT(CASE WHEN LOWER(i.customer) LIKE :cust2 THEN 1 END)
                FROM investments i JOIN investment_budget b ON i.row_id = b.row_id
            """, {"cust1": f"%{first}%", "cust2": f"%{second}%"}

    # Threshold (thr is a float — safe to interpolate)
    if itype == "threshold" and thr is not None:
        w, p = where([f"b.{fc} > {float(thr)}"])
        return f"""
            SELECT i.company, i.region, i.plant, i.investment_category,
                   i.investment_description, b.{fc} AS budget_k_eur
            {join} {w} ORDER BY b.{fc} DESC {lim or 'LIMIT 50'}
        """, p

    # By plant
    if itype == "by_plant":
        w, p = where()
        return f"""
            SELECT i.plant, i.company, i.region,
                   SUM(b.{fc}) AS budget_k_eur, COUNT(*) AS project_count
            {join} {w}
            GROUP BY i.plant, i.company, i.region
            ORDER BY budget_k_eur DESC {lim or 'LIMIT 10'}
        """, p

    # Top-N
    if itype == "top_n":
        w, p = where()
        if re.search(r'compan', q_norm):
            return f"""
                SELECT i.company, SUM(b.{fc}) AS budget_k_eur, COUNT(*) AS project_count
                {join} {w} GROUP BY i.company ORDER BY budget_k_eur DESC {lim or 'LIMIT 10'}
            """, p
        if re.search(r'region', q_norm):
            return f"""
                SELECT i.region, SUM(b.{fc}) AS budget_k_eur, COUNT(*) AS project_count
                {join} {w} GROUP BY i.region ORDER BY budget_k_eur DESC {lim or 'LIMIT 10'}
            """, p
        if re.search(r'plant', q_norm):
            return f"""
                SELECT i.plant, SUM(b.{fc}) AS budget_k_eur, COUNT(*) AS project_count
                {join} {w} GROUP BY i.plant ORDER BY budget_k_eur DESC {lim or 'LIMIT 10'}
            """, p
        if re.search(r'customer', q_norm):
            extra = "AND i.customer IS NOT NULL AND i.customer != 'Unknown'"
            clause = f"{w} {extra}" if w else f"WHERE i.customer IS NOT NULL AND i.customer != 'Unknown'"
            return f"""
                SELECT i.customer, SUM(b.{fc}) AS budget_k_eur, COUNT(*) AS project_count
                {join} {clause}
                GROUP BY i.customer ORDER BY budget_k_eur DESC {lim or 'LIMIT 10'}
            """, p
        return f"""
            SELECT i.plant, i.company, i.region, i.investment_category,
                   b.{fc} AS budget_k_eur, b.total_5y_k_eur
            {join} {w} ORDER BY b.{fc} DESC {lim or 'LIMIT 10'}
        """, p

    # By category
    if itype == "by_category" or (cat and not co and not reg and not cust):
        w, p = where()
        return f"""
            SELECT i.investment_category, COUNT(*) AS count, SUM(b.{fc}) AS budget_k_eur
            {join} {w} GROUP BY i.investment_category ORDER BY budget_k_eur DESC
        """, p

    # By company (budget)
    if itype == "by_company_budget":
        w, p = where()
        return f"""
            SELECT i.company, SUM(b.{fc}) AS budget_k_eur, COUNT(*) AS project_count
            {join} {w} GROUP BY i.company ORDER BY budget_k_eur DESC
        """, p

    # By region (budget)
    if itype == "by_region_budget":
        w, p = where()
        return f"""
            SELECT i.region, SUM(b.{fc}) AS budget_k_eur, COUNT(*) AS project_count
            {join} {w} GROUP BY i.region ORDER BY budget_k_eur DESC
        """, p

    # By customer (budget)
    if itype == "by_customer_budget":
        w, p = where(["i.customer IS NOT NULL", "i.customer != 'Unknown'"])
        return f"""
            SELECT i.customer, SUM(b.{fc}) AS budget_k_eur, COUNT(*) AS project_count
            {join} {w} GROUP BY i.customer ORDER BY budget_k_eur DESC {lim or 'LIMIT 10'}
        """, p

    # Plant-level query (e.g. "SMP Serbia")
    if ps:
        w, p = where()
        return f"""
            SELECT i.plant, i.company, i.region, COUNT(*) AS project_count,
                   SUM(b.{fc}) AS budget_k_eur, SUM(b.total_5y_k_eur) AS total_5y_k_eur
            {join} {w}
            GROUP BY i.plant, i.company, i.region ORDER BY budget_k_eur DESC
        """, p

    # Specific company + region
    if co and reg:
        _, p = where()
        return f"""
            SELECT i.company, i.region, COUNT(*) AS project_count,
                   SUM(b.{fc}) AS budget_k_eur, SUM(b.total_5y_k_eur) AS total_5y_k_eur
            {join} WHERE i.company = :company AND i.region = :region
            GROUP BY i.company, i.region
        """, p

    # Specific company
    if co:
        if cat:
            _, p = where()
            return f"""
                SELECT i.company, i.investment_category, COUNT(*) AS project_count,
                       SUM(b.{fc}) AS budget_k_eur, SUM(b.total_5y_k_eur) AS total_5y_k_eur
                FROM investments i JOIN investment_budget b ON i.row_id = b.row_id
                WHERE i.company = :company AND i.investment_category = :category
                GROUP BY i.company, i.investment_category ORDER BY budget_k_eur DESC
            """, p
        # If asking for a single total (not breakdown), return one row
        if re.search(r'\\b(total|sum|gesamt|kul|overall|how much|kitna|budget)\\b', q_norm) and itype == "general":
            return f"""
                SELECT i.company,
                       SUM(b.{fc}) AS budget_k_eur,
                       COUNT(*) AS project_count,
                       SUM(b.total_5y_k_eur) AS total_5y_k_eur
                {join} WHERE i.company = :company
                GROUP BY i.company
            """, {"company": co}
        return f"""
            SELECT i.company, i.investment_category, COUNT(*) AS project_count,
                   SUM(b.{fc}) AS budget_k_eur, SUM(b.total_5y_k_eur) AS total_5y_k_eur
            {join} WHERE i.company = :company
            GROUP BY i.company, i.investment_category ORDER BY budget_k_eur DESC
        """, {"company": co}

    # Specific region
    if reg:
        if cat:
            _, p = where()
            return f"""
                SELECT i.region, i.investment_category, COUNT(*) AS project_count,
                       SUM(b.{fc}) AS budget_k_eur
                FROM investments i JOIN investment_budget b ON i.row_id = b.row_id
                WHERE i.region = :region AND i.investment_category = :category
                GROUP BY i.region, i.investment_category ORDER BY budget_k_eur DESC
            """, p
        return f"""
            SELECT i.region, i.company, COUNT(*) AS project_count,
                   SUM(b.{fc}) AS budget_k_eur, SUM(b.total_5y_k_eur) AS total_5y_k_eur
            {join} WHERE i.region = :region
            GROUP BY i.region, i.company ORDER BY budget_k_eur DESC
        """, {"region": reg}

    # Specific customer
    if cust:
        return f"""
            SELECT i.company, i.region, COUNT(*) AS project_count, SUM(b.{fc}) AS budget_k_eur
            {join} WHERE LOWER(i.customer) LIKE :customer
            GROUP BY i.company, i.region ORDER BY budget_k_eur DESC
        """, {"customer": f"%{cust.lower()}%"}

    # Cashflow
    if itype == "cashflow" or re.search(r'\b(monthly|cashflow|month.wise)\b', q_norm):
        if re.search(r'2025/26|2526|this year|current', q_norm):
            return """
                SELECT period_date, SUM(amount_k_eur) AS amount_k_eur
                FROM investment_monthly_cashflow WHERE fiscal_year = :fy
                GROUP BY period_date ORDER BY period_date
            """, {"fy": "2025/26"}
        if re.search(r'2026/27|2627|next year', q_norm):
            return """
                SELECT period_date, SUM(amount_k_eur) AS amount_k_eur
                FROM investment_monthly_cashflow WHERE fiscal_year = :fy
                GROUP BY period_date ORDER BY period_date
            """, {"fy": "2026/27"}
        return """
            SELECT period_date, SUM(amount_k_eur) AS amount_k_eur
            FROM investment_monthly_cashflow
            GROUP BY period_date ORDER BY period_date
        """, {}

    # 5-year total
    if fc == "total_5y_k_eur":
        return TRUE_5Y_SQL, {}

    # Tangible/intangible
    if re.search(r'\b(tangible|intangible)\b', q_norm):
        return f"""
            SELECT i.tangible_intangible, COUNT(*) AS count, SUM(b.{fc}) AS budget_k_eur
            {join} GROUP BY i.tangible_intangible ORDER BY budget_k_eur DESC
        """, {}

    # Funding source
    if re.search(r'\b(funding|source|leasing|borrowing)\b', q_norm):
        return f"""
            SELECT i.source_of_funding, COUNT(*) AS count, SUM(b.{fc}) AS budget_k_eur
            {join} GROUP BY i.source_of_funding ORDER BY budget_k_eur DESC
        """, {}

    # Simple total budget
    if re.search(r'\b(total|sum|gesamt|kul|overall|budget)\b', q_norm):
        return f"SELECT SUM({fc}) AS total_k_eur FROM investment_budget", {}

    # Generic count
    if re.search(r'\b(how many|count|kitne)\b', q_norm):
        return "SELECT COUNT(*) AS total_investments FROM investments", {}

    # Fallback for explicit non-current FY
    if fc in ("budget_fy_2627", "plan_fy_2728", "plan_fy_2829", "plan_fy_2930"):
        return f"SELECT SUM({fc}) AS total_k_eur FROM investment_budget", {}

    return None


# ── GENERATE INSIGHT (fully multilingual) ──────────────────

def generate_insight(df: pd.DataFrame, intent: dict, q_norm: str, lang: str) -> str:
    if df is None or df.empty:
        return ""
    try:
        rows  = len(df)
        cols  = list(df.columns)
        fc    = intent.get("fy_col") or "budget_fy_2526"
        fyl   = _fy_label(fc, lang)

        # Find best numeric col — priority: financial/budget cols FIRST, count cols as fallback
        num_col = next((c for c in cols if any(x in c.lower() for x in
                        ["budget","plan","total","eur","amount","value","avg"]
                        ) and pd.to_numeric(df[c], errors="coerce").notna().any()), None)
        # Fallback to count column only if no financial column found
        if num_col is None:
            num_col = next((c for c in cols if "count" in c.lower()
                           and pd.to_numeric(df[c], errors="coerce").notna().any()), None)

        # Find VARYING categorical col
        cat_col = None
        for c in cols:
            if c == num_col: continue
            if pd.to_numeric(df[c], errors="coerce").notna().all(): continue
            if df[c].nunique() > 1:
                cat_col = c
                break
        if cat_col is None:
            cat_col = next((c for c in cols if not pd.to_numeric(df[c], errors="coerce").notna().all()), cols[0])

        cnt_col = next((c for c in cols if "count" in c.lower() and c != num_col), None)

        # Single scalar
        if df.shape == (1, 1):
            val = df.iloc[0, 0]
            col = cols[0].lower()
            if "count" in col or "investment" in col:
                n = int(float(val))
                return {
                    "english":  f"**{n:,}** investments in total.",
                    "hindi":    f"कुल **{n:,}** निवेश हैं।",
                    "hinglish": f"Total **{n:,}** investments hain!",
                    "german":   f"Insgesamt **{n:,}** Investitionen.",
                }.get(lang, f"**{n:,}** investments in total.")
            else:
                fv = format_large_number(val)
                return {
                    "english":  f"Total: **{fv}** ({fyl})",
                    "hindi":    f"कुल: **{fv}** ({fyl})",
                    "hinglish": f"Total banta hai: **{fv}** ({fyl})",
                    "german":   f"Gesamt: **{fv}** ({fyl})",
                }.get(lang, f"Total: **{fv}** ({fyl})")

        if not num_col or not cat_col:
            return {
                "english":  f"_{rows} records returned._",
                "hindi":    f"_{rows} रिकॉर्ड मिले।_",
                "hinglish": f"_{rows} records mile._",
                "german":   f"_{rows} Einträge._",
            }.get(lang, f"_{rows} records returned._")

        numeric  = pd.to_numeric(df[num_col], errors="coerce").fillna(0)
        total    = numeric.sum()
        top_row  = df.iloc[0]
        top_val  = float(top_row[num_col]) if num_col in top_row.index else 0
        # Format period_date as "Month YYYY" for cashflow queries
        def _fmt_date(val: str) -> str:
            try:
                from datetime import datetime
                return datetime.strptime(str(val), "%Y-%m-%d").strftime("%B %Y")
            except Exception:
                return str(val)
        top_lbl = _fmt_date(top_row[cat_col]) if cat_col == "period_date" else str(top_row[cat_col])
        top_pct  = (top_val / total * 100) if total > 0 else 0
        is_count = "count" in num_col.lower() or "investment_count" in num_col.lower()

        lines = []
        if total > 0:
            if is_count:
                lines.append({
                    "english":  f"**{top_lbl}** leads with **{int(top_val):,}** investments ({top_pct:.1f}% of {int(total):,} total).",
                    "hindi":    f"**{top_lbl}** सबसे आगे — **{int(top_val):,}** निवेश ({top_pct:.1f}%)।",
                    "hinglish": f"**{top_lbl}** top pe hai — **{int(top_val):,}** investments ({top_pct:.1f}%).",
                    "german":   f"**{top_lbl}** führt mit **{int(top_val):,}** Investitionen ({top_pct:.1f}%).",
                }.get(lang, f"**{top_lbl}** leads with **{int(top_val):,}**."))
            else:
                lines.append({
                    "english":  f"**{top_lbl}** leads with **{fmt(top_val)}** ({top_pct:.1f}% of {fyl} total of {fmt(total)}).",
                    "hindi":    f"**{top_lbl}** सबसे आगे — **{fmt(top_val)}** ({fyl} कुल {fmt(total)} का {top_pct:.1f}%)।",
                    "hinglish": f"**{top_lbl}** top pe — **{fmt(top_val)}** ({fyl} total {fmt(total)} ka {top_pct:.1f}%).",
                    "german":   f"**{top_lbl}** führt mit **{fmt(top_val)}** ({top_pct:.1f}% des {fyl}-Gesamts von {fmt(total)}).",
                }.get(lang, f"**{top_lbl}** leads with **{fmt(top_val)}**."))

        if rows >= 2:
            r2  = df.iloc[1]
            v2  = float(r2[num_col]) if num_col in r2.index else 0
            l2  = _fmt_date(r2[cat_col]) if cat_col == "period_date" else str(r2[cat_col])
            p2  = (v2 / total * 100) if total > 0 else 0
            v2s = f"{int(v2):,}" if is_count else fmt(v2)
            lines.append({
                "english":  f"**{l2}** is second at **{v2s}** ({p2:.1f}%).",
                "hindi":    f"**{l2}** दूसरे स्थान पर — **{v2s}** ({p2:.1f}%)।",
                "hinglish": f"**{l2}** second hai — **{v2s}** ({p2:.1f}%).",
                "german":   f"**{l2}** ist Zweiter mit **{v2s}** ({p2:.1f}%).",
            }.get(lang, f"**{l2}** is second at **{v2s}**."))

        if cnt_col and cnt_col in df.columns and not is_count:
            top_cnt = int(top_row[cnt_col]) if cnt_col in top_row.index else 0
            lines.append({
                "english":  f"_{rows} groups · {top_lbl} has {top_cnt:,} projects._",
                "hindi":    f"_{rows} समूह · {top_lbl} के {top_cnt:,} प्रोजेक्ट।_",
                "hinglish": f"_{rows} groups · {top_lbl} ke {top_cnt:,} projects._",
                "german":   f"_{rows} Gruppen · {top_lbl} hat {top_cnt:,} Projekte._",
            }.get(lang, f"_{rows} groups._"))
        else:
            lines.append({
                "english":  f"_{rows} records returned._",
                "hindi":    f"_{rows} रिकॉर्ड मिले।_",
                "hinglish": f"_{rows} records mile._",
                "german":   f"_{rows} Einträge._",
            }.get(lang, f"_{rows} records._"))

        # Comparison special
        _FY_LABEL_MAP = {
            "budget_fy_2526": "FY 2025/26", "budget_fy_2627": "FY 2026/27",
            "plan_fy_2728": "FY 2027/28",   "plan_fy_2829": "FY 2028/29",
            "plan_fy_2930": "FY 2029/30",
        }
        if rows == 2 and intent.get("compare") and num_col:
            v1 = float(df.iloc[0][num_col])
            v2 = float(df.iloc[1][num_col])
            l1 = str(df.iloc[0][cat_col])
            l2 = str(df.iloc[1][cat_col])
            # Map raw DB column names to human-readable FY labels
            l1 = _FY_LABEL_MAP.get(l1, l1)
            l2 = _FY_LABEL_MAP.get(l2, l2)
            diff = abs(v1 - v2)
            if v2 > 0:
                ratio  = v1 / v2
                bigger = l1 if v1 > v2 else l2
                lines  = [{
                    "english":  f"**{l1}** ({fmt(v1)}) vs **{l2}** ({fmt(v2)}) — **{bigger}** is **{ratio:.1f}×** larger, difference of {fmt(diff)}.",
                    "hindi":    f"**{l1}** ({fmt(v1)}) बनाम **{l2}** ({fmt(v2)}) — **{bigger}** **{ratio:.1f}×** बड़ा, अंतर {fmt(diff)}।",
                    "hinglish": f"**{l1}** ({fmt(v1)}) vs **{l2}** ({fmt(v2)}) — **{bigger}** **{ratio:.1f}×** bada, difference {fmt(diff)}.",
                    "german":   f"**{l1}** ({fmt(v1)}) vs. **{l2}** ({fmt(v2)}) — **{bigger}** ist **{ratio:.1f}×** größer, Differenz {fmt(diff)}.",
                }.get(lang, f"**{l1}** vs **{l2}**")]

        return "\n\n".join(lines) if lines else {
            "english":  f"_{rows} records returned._",
            "hindi":    f"_{rows} रिकॉर्ड।_",
            "hinglish": f"_{rows} records._",
            "german":   f"_{rows} Einträge._",
        }.get(lang, f"_{rows} records._")

    except Exception:
        return f"_{len(df)} records returned._"


# ── CHART BUILDER ──────────────────────────────────────────

def build_chart(df: pd.DataFrame, intent: dict):
    try:
        if df is None or len(df) == 0 or len(df.columns) < 2:
            return None
        # No chart for raw list_detail
        if intent.get("intent_type") == "list_detail":
            return None

        # Find varying categorical label col
        cat_col = None
        for c in df.columns:
            if pd.to_numeric(df[c], errors="coerce").notna().all(): continue
            if df[c].nunique() > 1:
                cat_col = c; break
        if cat_col is None:
            cat_col = df.columns[0]

        num_col = next(
            (c for c in df.columns if c != cat_col and
             pd.to_numeric(df[c], errors="coerce").notna().any()), None
        )
        if num_col is None:
            return None

        labels = df[cat_col].astype(str).tolist()
        values = pd.to_numeric(df[num_col], errors="coerce").fillna(0).tolist()
        n      = len(df)
        itype  = intent.get("intent_type", "")

        if intent.get("requested_chart"):
            chart_type = intent["requested_chart"]
        elif itype == "cashflow" or "date" in cat_col.lower():
            chart_type = "line"
        elif itype == "compare":
            chart_type = "bar"
        elif n <= 6 and itype not in ("top_n","by_company_count","by_region_count","average"):
            chart_type = "pie"
        else:
            chart_type = "bar"

        return {
            "labels":     labels,
            "values":     values,
            "label_col":  str(cat_col),
            "value_col":  str(num_col),
            "chart_type": chart_type,
        }
    except Exception:
        return None


# ── CSV EXPORT ─────────────────────────────────────────────

def to_csv_bytes(df: pd.DataFrame) -> bytes | None:
    if df is None or df.empty: return None
    try:
        return df.to_csv(index=False).encode("utf-8")
    except Exception:
        return None


# ── CHATBOT CLASS ──────────────────────────────────────────

class InvestmentChatbot:

    def __init__(self, engine):
        self.engine           = engine
        self.cache            = {}
        self._memory: deque   = deque(maxlen=10)
        self._pending         = None   # stores original_q waiting for year

        # Detect which LLM is available — Gemini (cloud) or Ollama (local)
        self.gemini_model     = None   # Gemini client (set below if available)
        self.llm_provider     = "none" # "gemini" | "ollama" | "none"

        # Try Gemini first (cloud environment)
        gemini_key = self._get_gemini_key()
        if _GEMINI_SDK_AVAILABLE and gemini_key:
            try:
                genai.configure(api_key=gemini_key)
                self.gemini_model = genai.GenerativeModel("gemini-1.5-flash")
                self.llm_provider = "gemini"
            except Exception as e:
                print(f"[LLM] Gemini setup failed: {e}")

        # Fallback to Ollama (local environment)
        if self.llm_provider == "none" and self._check_ollama():
            self.llm_provider = "ollama"

        # Backward-compat flag used by app.py for the sidebar warning banner
        self.ollama_available = (self.llm_provider != "none")

    def _get_gemini_key(self) -> str | None:
        """Fetch Gemini API key from Streamlit secrets or environment."""
        # Try Streamlit secrets first (cloud deployment)
        try:
            import streamlit as st
            if "GEMINI_API_KEY" in st.secrets:
                return st.secrets["GEMINI_API_KEY"]
        except Exception:
            pass
        # Fallback to environment variable (local .env)
        return os.getenv("GEMINI_API_KEY")

    def _check_ollama(self) -> bool:
        """Check if Ollama is running locally with mistral available."""
        try:
            r = requests.get("http://localhost:11434/api/tags", timeout=2)
            if r.status_code != 200:
                return False
            # Verify mistral model is pulled
            models = r.json().get("models", [])
            has_mistral = any("mistral" in m.get("name", "").lower() for m in models)
            return has_mistral
        except Exception:
            return False

    def _key(self, q): return hashlib.md5(q.lower().strip().encode()).hexdigest()

    def _run_sql(self, sql: str, params: dict | None = None):
        try:
            with self.engine.connect() as conn:
                res = conn.execute(text(sql), params or {})
                return pd.DataFrame(res.fetchall(), columns=res.keys())
        except Exception as e:
            print(f"[SQL ERROR] {e}\n{sql}")
            return None

    def _llm_sql(self, question: str, lang: str = "english") -> str | None:
        """
        Generate SQL via Ollama/Mistral for questions the rule-based
        builder could not handle.

        Improvements vs v1:
        - Language is passed explicitly so the model knows the input language
        - Normalized (English-ish) version is included alongside the original
          so Mistral does not have to translate German/Hindi alone
        - Conversation history is trimmed to last 3 turns to stay within context
        - Response is cleaned more aggressively before returning
        """
        if not self.ollama_available:
            return None

        # Normalize the question (applies WORD_MAP/PHRASE_MAP translations)
        q_norm = normalise(question.lower().strip())

        # Last 3 turns of conversation context (6 messages max)
        recent = list(self._memory)[-6:]
        ctx = "\n".join(
            f"{'User' if m['role']=='user' else 'Bot'}: {m['content']}"
            for m in recent
        ) or "None"

        lang_hint = {
            "german":   "The question is in German. Translate mentally, then write SQL.",
            "hindi":    "The question is in Hindi. Translate mentally, then write SQL.",
            "hinglish": "The question is in Hinglish (Hindi+English mix). Write SQL.",
        }.get(lang, "The question is in English.")

        prompt = f"""{SCHEMA}

LANGUAGE NOTE: {lang_hint}

RECENT CONVERSATION:
{ctx}

Original question : {question}
Normalised (hints): {q_norm}

Write a single valid PostgreSQL SELECT query. Return ONLY the SQL, nothing else.
SQL:"""
        try:
            r = requests.post(
                "http://localhost:11434/api/generate",
                json={"model": "mistral", "prompt": prompt, "stream": False},
                timeout=30,
            )
            sql = r.json().get("response", "").strip()
            # Strip markdown fences, leading/trailing noise
            sql = re.sub(r"```sql|```", "", sql).strip()
            sql = re.sub(r"^(sql|query)[:\s]+", "", sql, flags=re.I).strip()
            # Take only the first statement
            if ";" in sql:
                sql = sql.split(";")[0].strip()
            # Reject if model returned the unsupported sentinel or empty
            if not sql or "unsupported" in sql.lower():
                return None
            return sql
        except Exception:
            return None

    def _resolve_clarification(self, q: str) -> str | None:
        """
        NEW-7: robust year resolver — always clears pending (no loops).
        Returns resolved_col string or None.
        """
        if not self._pending: return None
        orig = self._pending.get("original_q","")
        self._pending = None   # always clear — never loop

        ql = q.lower().strip()

        # Check 5-year signals FIRST — must not be overridden by bare "2025" substring
        if re.search(r'(5[\s\-]?year|5y|full\s*5|five[\s\-]year|all year|whole|full plan|poora|panch|2025.2030|2025\u20132030)', ql):
            return f"__RESOLVED__total_5y_k_eur__{orig}"

        # Scan FY_COL_MAP — take LAST match so more specific tokens win
        resolved_col = None
        for token, col in FY_COL_MAP.items():
            if token in ql:
                resolved_col = col
        if resolved_col:
            return f"__RESOLVED__{resolved_col}__{orig}"

        # Broad patterns as final fallback
        if re.search(r'(fy\s*2025|2025/26|2526|\bcurrent\b|this year)', ql):
            return f"__RESOLVED__budget_fy_2526__{orig}"
        if re.search(r'(fy\s*2026|2026/27|2627|\bnext\b|next year)', ql):
            return f"__RESOLVED__budget_fy_2627__{orig}"
        if re.search(r'(fy\s*2027|2027/28|2728)', ql):
            return f"__RESOLVED__plan_fy_2728__{orig}"
        if re.search(r'(fy\s*2028|2028/29|2829)', ql):
            return f"__RESOLVED__plan_fy_2829__{orig}"
        if re.search(r'(fy\s*2029|2029/30|2930)', ql):
            return f"__RESOLVED__plan_fy_2930__{orig}"

        return None

    def _mem(self, user_q, bot_ans):
        self._memory.append({"role":"user","content":user_q})
        self._memory.append({"role":"assistant","content":bot_ans})

    def clear_memory(self):
        self._memory.clear()
        self._pending = None
        self.cache.clear()

    def _result(self, answer, sql, df, clarification, error=None):
        fmt_df = format_df_for_display(df)
        return {
            "answer":        answer,
            "sql":           sql,
            "chart_data":    build_chart(df, self._last_intent) if df is not None else None,
            "rows":          len(df) if df is not None else 0,
            "error":         error,
            "clarification": clarification,
            "csv_data":      to_csv_bytes(df),
            "df":            fmt_df,
        }

    # ── MAIN ──────────────────────────────────────────────
    def ask(self, question: str) -> dict:
        self._last_intent = {}
        try:
            # NEW-1: detect language per message — not locked to session
            lang  = detect_lang(question)
            q_raw = question.lower().strip()

            # SECURITY: block DML keywords immediately — before any intent processing
            # This prevents "Delete all X" from triggering clarification flows
            if re.search(r'\b(delete|drop|truncate|insert|update|alter|create)\b', q_raw):
                res = self._result(R("unsafe", lang), None, None, False)
                self._mem(question, res["answer"])
                return res

            # Resolve pending year clarification
            resolved = self._resolve_clarification(q_raw)
            if resolved and resolved.startswith("__RESOLVED__"):
                parts        = resolved.split("__", 3)
                resolved_col = parts[2]
                original_q   = parts[3] if len(parts) > 3 else question
                # Re-process original question with year now known
                question = original_q
                q_raw    = original_q.lower().strip()
                q_norm   = normalise(q_raw)
                intent   = parse_intent(q_norm, q_raw)
                intent["fy_col"] = resolved_col   # inject resolved year
                self._last_intent = intent
                built = build_sql(intent, q_norm, q_raw)
                if built:
                    sql, params = built
                else:
                    sql = self._llm_sql(question, lang)
                    if sql: sql = sanitise(sql)
                    params = {}
                if not is_safe(sql):
                    res = self._result(R("unsafe",lang), sql, None, False)
                    self._mem(question, res["answer"])
                    return res
                df = self._run_sql(sql, params)
                if df is None:
                    res = self._result(R("sql_error",lang), sql, None, False, "SQL failed")
                    self._mem(question, res["answer"])
                    return res
                if df.empty:
                    res = self._result(R("no_data",lang), sql, None, False)
                    self._mem(question, res["answer"])
                    return res
                answer = generate_insight(df, intent, q_norm, lang)
                res    = self._result(answer, sql, df, False)
                self._mem(question, answer)
                return res
            elif resolved:
                question = resolved
                q_raw    = resolved.lower()

            q_norm = normalise(q_raw)

            # Cache
            ck = self._key(question)
            if ck in self.cache:
                return self.cache[ck]

            # Out-of-scope
            if is_oos(q_norm):
                res = self._result(R("out_of_scope",lang), None, None, False)
                self._mem(question, res["answer"])
                return res

            # Actuals check BEFORE safety
            if re.search(r'\b(actual|spent|disbursed|utilized)\b', q_norm) and \
               re.search(r'\b(invest|budget|spend|amount|exceed)\b', q_norm):
                res = self._result(R("no_actuals",lang), None, None, True)
                self._mem(question, res["answer"])
                return res

            # Status check
            if re.search(r'\b(status|approved|started|completed|not started|in progress)\b', q_norm):
                res = self._result(R("no_status",lang), None, None, True)
                self._mem(question, res["answer"])
                return res

            # Vague "show me investments" with no filter
            if re.search(r'^(show|list|display|zeig)\s+(me\s+|mir\s+)?(all\s+|alle\s+)?investments?\??$', q_norm.strip()):
                res = self._result(R("clarify_vague_show",lang), None, None, True)
                self._mem(question, res["answer"])
                return res

            # Vague "what is the total" with no context
            if re.search(r'^(what is (the )?total\??|total\??)$', q_norm.strip()):
                res = self._result(R("clarify_vague_total",lang), None, None, True)
                self._mem(question, res["answer"])
                return res

            # Parse intent
            intent = parse_intent(q_norm, q_raw)
            self._last_intent = intent

            # NEW-2: Financial intent without year → ask which year
            if needs_year(intent):
                self._pending = {"original_q": question}
                res = self._result(R("clarify_year",lang), None, None, True)
                self._mem(question, res["answer"])
                return res

            # Build SQL
            built = build_sql(intent, q_norm, q_raw)
            if built:
                sql, params = built
            else:
                sql = self._llm_sql(question, lang)
                if sql: sql = sanitise(sql)
                params = {}

            # Safety
            if not is_safe(sql):
                res = self._result(R("unsafe",lang), sql, None, False)
                self._mem(question, res["answer"])
                self.cache[ck] = res
                return res

            # Execute
            df = self._run_sql(sql, params)

            if df is None:
                res = self._result(R("sql_error",lang), sql, None, False, "SQL failed")
                self._mem(question, res["answer"])
                return res

            if df.empty:
                res = self._result(R("no_data",lang), sql, None, False)
                self._mem(question, res["answer"])
                self.cache[ck] = res
                return res

            answer = generate_insight(df, intent, q_norm, lang)
            res    = self._result(answer, sql, df, False)
            self._mem(question, answer)
            self.cache[ck] = res
            return res

        except Exception as e:
            lang = detect_lang(question) if question else "english"
            return self._result(R("internal_error",lang), None, None, False, str(e))
