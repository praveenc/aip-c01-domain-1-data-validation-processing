"""AWS Glue Data Quality rules for ALL healthcare tables.

Demonstrates:
  - Defining DQDL (Data Quality Definition Language) rules for 3 table types:
    1. patient_records_csv  — flat tabular CSV data
    2. patient_records_json — nested JSON with arrays and structs
    3. clinical_notes_json  — unstructured text notes
  - Running rules locally for testing (simulated) and via Glue DQ API
  - Generating quality reports with pass/fail metrics
  - Handling nested JSON data (arrays, structs) in DQDL via CustomSql

Covers requirement 1.1: AWS Glue Data Quality for automated data quality checks
"""

import json
import csv
import re
import statistics
from datetime import datetime
from pathlib import Path

OUTPUT_DIR = Path(__file__).parent / "output"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
DATA_DIR = Path(__file__).parent.parent / "synth_data" / "output"

# ---------------------------------------------------------------------------
# DQDL Rule Definitions — Patient Records CSV (flat tabular)
# ---------------------------------------------------------------------------
GLUE_DQ_RULESET_CSV = """
Rules = [
    # --- COMPLETENESS RULES ---
    Completeness "patient_name" >= 0.98,
    Completeness "mrn" = 1.0,
    Completeness "admission_date" = 1.0,
    Completeness "discharge_date" = 1.0,
    Completeness "primary_icd10" >= 0.95,
    Completeness "systolic_bp" >= 0.90,
    Completeness "diastolic_bp" >= 0.90,

    # --- UNIQUENESS RULES ---
    Uniqueness "record_id" = 1.0,

    # --- CONSISTENCY RULES ---
    CustomSql "SELECT COUNT(*) FROM primary WHERE discharge_date < admission_date" = 0,
    ColumnValues "length_of_stay_days" >= 0,

    # --- ACCURACY / RANGE RULES ---
    ColumnValues "age" between 0 and 120,
    ColumnValues "systolic_bp" between 50 and 250,
    ColumnValues "diastolic_bp" between 30 and 150,
    ColumnValues "heart_rate" between 20 and 250,
    ColumnValues "temperature_f" between 90.0 and 108.0,

    # --- FORMAT RULES ---
    ColumnValues "primary_icd10" matches "[A-Z][0-9]{2}(\\.[0-9]{1,2})?"
        with threshold >= 0.90,

    # --- STATISTICAL RULES ---
    Mean "age" between 40 and 80,
    StandardDeviation "length_of_stay_days" <= 15,
    RowCount >= 100
]

Analyzers = [
    RowCount,
    Completeness "patient_name",
    Completeness "mrn",
    Completeness "primary_icd10",
    ColumnLength "primary_icd10",
    DistinctValuesCount "department",
    DistinctValuesCount "primary_icd10",
    Mean "age",
    Mean "systolic_bp",
    StandardDeviation "heart_rate"
]
"""

# ---------------------------------------------------------------------------
# DQDL Rule Definitions — Patient Records JSON (nested JSON)
# ---------------------------------------------------------------------------
GLUE_DQ_RULESET_JSON = """
Rules = [
    # --- COMPLETENESS (top-level) ---
    Completeness "patient_name" >= 0.98,
    Completeness "mrn" = 1.0,
    Completeness "record_id" = 1.0,
    Completeness "admission_date" = 1.0,
    Completeness "discharge_date" = 1.0,
    Completeness "attending_physician" = 1.0,
    Completeness "department" = 1.0,

    # --- COMPLETENESS (nested fields) ---
    # IMPORTANT: DQDL Completeness does NOT support array/struct types.
    # Use CustomSql IS NULL checks instead.
    CustomSql "SELECT COUNT(*) FROM primary WHERE diagnoses IS NULL" <= 10,
    CustomSql "SELECT COUNT(*) FROM primary WHERE medications IS NULL" <= 20,
    CustomSql "SELECT COUNT(*) FROM primary WHERE vitals_at_discharge IS NULL" <= 20,

    # --- UNIQUENESS ---
    Uniqueness "record_id" = 1.0,

    # --- RANGE RULES ---
    ColumnValues "age" between 0 and 120,
    ColumnValues "length_of_stay_days" >= 0,

    # --- VALUE RULES ---
    ColumnValues "gender" in ["M", "F"],

    # --- CONSISTENCY ---
    CustomSql "SELECT COUNT(*) FROM primary WHERE discharge_date < admission_date" = 0,

    # --- NESTED: diagnoses array (using Spark SQL size()) ---
    CustomSql "SELECT COUNT(*) FROM primary WHERE size(diagnoses) = 0" = 0,

    # --- NESTED: medications array ---
    CustomSql "SELECT COUNT(*) FROM primary WHERE size(medications) = 0" <= 10,

    # --- NESTED: vitals struct (dot notation for struct fields) ---
    CustomSql "SELECT COUNT(*) FROM primary WHERE vitals_at_discharge.systolic_bp IS NOT NULL AND (vitals_at_discharge.systolic_bp < 50 OR vitals_at_discharge.systolic_bp > 250)" = 0,
    CustomSql "SELECT COUNT(*) FROM primary WHERE vitals_at_discharge.heart_rate IS NOT NULL AND vitals_at_discharge.heart_rate < 0" = 0,
    CustomSql "SELECT COUNT(*) FROM primary WHERE vitals_at_discharge.temperature_f IS NOT NULL AND (vitals_at_discharge.temperature_f < 90.0 OR vitals_at_discharge.temperature_f > 108.0)" = 0,

    # --- STATISTICAL ---
    Mean "age" between 40 and 80,
    StandardDeviation "length_of_stay_days" <= 15,
    RowCount >= 100
]

Analyzers = [
    RowCount,
    Completeness "patient_name",
    Completeness "mrn",
    Mean "age",
    StandardDeviation "age",
    Mean "length_of_stay_days",
    DistinctValuesCount "department",
    DistinctValuesCount "gender"
]
"""

# ---------------------------------------------------------------------------
# DQDL Rule Definitions — Clinical Notes JSON (unstructured text)
# ---------------------------------------------------------------------------
GLUE_DQ_RULESET_NOTES = """
Rules = [
    # --- COMPLETENESS ---
    Completeness "note_text" = 1.0,
    Completeness "record_id" = 1.0,
    Completeness "mrn" = 1.0,
    Completeness "note_type" = 1.0,
    Completeness "author" = 1.0,
    Completeness "date" = 1.0,

    # --- UNIQUENESS ---
    Uniqueness "record_id" = 1.0,

    # --- VALUE RULES ---
    ColumnValues "note_type" in ["Discharge Summary", "Progress Note", "H&P"],

    # --- TEXT QUALITY ---
    ColumnLength "note_text" >= 20,

    # --- FORMAT RULES ---
    ColumnValues "date" matches "[0-9]{4}-[0-9]{2}-[0-9]{2}"
        with threshold >= 0.95,
    ColumnValues "mrn" matches "MRN-[0-9]{6}"
        with threshold >= 0.95,
    ColumnValues "author" matches "Dr\\. [A-Z][a-z]+"
        with threshold >= 0.90,

    # --- STATISTICAL ---
    RowCount >= 100,
    RowCount = 200
]

Analyzers = [
    RowCount,
    Completeness "note_text",
    Completeness "mrn",
    Completeness "author",
    ColumnLength "note_text",
    DistinctValuesCount "note_type",
    DistinctValuesCount "author"
]
"""

# Keep backward-compatible alias
GLUE_DQ_RULESET = GLUE_DQ_RULESET_CSV


# ═══════════════════════════════════════════════════════════════════════════════
# LOCAL SIMULATION — CSV Rules
# ═══════════════════════════════════════════════════════════════════════════════

def load_csv_records(csv_path: str) -> list[dict]:
    """Load patient records from CSV."""
    records = []
    with open(csv_path, "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            for field in ["age", "record_id", "length_of_stay_days", "num_medications"]:
                try:
                    row[field] = int(row[field]) if row[field] else None
                except (ValueError, KeyError):
                    row[field] = None
            for field in ["systolic_bp", "diastolic_bp", "heart_rate"]:
                try:
                    row[field] = int(row[field]) if row[field] else None
                except (ValueError, KeyError):
                    row[field] = None
            try:
                row["temperature_f"] = float(row["temperature_f"]) if row["temperature_f"] else None
            except (ValueError, KeyError):
                row["temperature_f"] = None
            records.append(row)
    return records


def load_json_records(json_path: str) -> list[dict]:
    """Load records from JSON array file."""
    with open(json_path, "r") as f:
        return json.load(f)


def check_completeness(records: list[dict], column: str, threshold: float) -> dict:
    """Check what fraction of records have a non-null, non-empty value."""
    total = len(records)
    non_null = sum(1 for r in records if r.get(column) not in [None, "", "None"])
    actual = non_null / total if total > 0 else 0
    return {
        "rule": f'Completeness "{column}" >= {threshold}',
        "type": "Completeness",
        "column": column,
        "threshold": threshold,
        "actual": round(actual, 4),
        "passed": actual >= threshold,
    }


def check_column_range(records: list[dict], column: str, min_val, max_val) -> dict:
    """Check that all non-null values fall within [min_val, max_val]."""
    violations = []
    checked = 0
    for r in records:
        val = r.get(column)
        if val is None:
            continue
        checked += 1
        if val < min_val or val > max_val:
            violations.append({"record_id": r.get("record_id"), "value": val})
    return {
        "rule": f'ColumnValues "{column}" between {min_val} and {max_val}',
        "type": "Range",
        "column": column,
        "range": [min_val, max_val],
        "records_checked": checked,
        "violations": len(violations),
        "violation_details": violations[:10],
        "passed": len(violations) == 0,
    }


def check_uniqueness(records: list[dict], column: str) -> dict:
    """Check uniqueness of a column."""
    values = [r.get(column) for r in records if r.get(column) is not None]
    unique_count = len(set(values))
    total = len(values)
    ratio = unique_count / total if total > 0 else 0
    return {
        "rule": f'Uniqueness "{column}" = 1.0',
        "type": "Uniqueness",
        "column": column,
        "total": total,
        "unique": unique_count,
        "ratio": round(ratio, 4),
        "passed": unique_count == total,
    }


def check_date_consistency(records: list[dict]) -> dict:
    """Check that discharge_date >= admission_date."""
    violations = []
    for r in records:
        try:
            admit = datetime.strptime(r["admission_date"], "%Y-%m-%d")
            discharge = datetime.strptime(r["discharge_date"], "%Y-%m-%d")
            if discharge < admit:
                violations.append({
                    "record_id": r.get("record_id"),
                    "admission_date": r["admission_date"],
                    "discharge_date": r["discharge_date"],
                })
        except (ValueError, KeyError):
            pass
    return {
        "rule": "CustomSql: discharge_date >= admission_date",
        "type": "Consistency",
        "violations": len(violations),
        "violation_details": violations[:10],
        "passed": len(violations) == 0,
    }


def check_icd10_format(records: list[dict]) -> dict:
    """Validate ICD-10 code format."""
    pattern = re.compile(r"^[A-Z]\d{2}(\.\d{1,2})?$")
    total = 0
    valid = 0
    violations = []
    for r in records:
        code = r.get("primary_icd10", "")
        if not code:
            continue
        total += 1
        if pattern.match(code):
            valid += 1
        else:
            violations.append({"record_id": r.get("record_id"), "icd10": code})
    ratio = valid / total if total > 0 else 0
    return {
        "rule": 'ColumnValues "primary_icd10" matches ICD-10 pattern (threshold >= 0.90)',
        "type": "Format",
        "total_checked": total,
        "valid": valid,
        "ratio": round(ratio, 4),
        "violations": violations[:10],
        "passed": ratio >= 0.90,
    }


def check_column_values_in(records: list[dict], column: str, allowed: list) -> dict:
    """Check that all non-null values are in the allowed set."""
    violations = []
    checked = 0
    for r in records:
        val = r.get(column)
        if val is None:
            continue
        checked += 1
        if val not in allowed:
            violations.append({"record_id": r.get("record_id"), "value": val})
    return {
        "rule": f'ColumnValues "{column}" in {allowed}',
        "type": "ValueSet",
        "column": column,
        "records_checked": checked,
        "violations": len(violations),
        "violation_details": violations[:10],
        "passed": len(violations) == 0,
    }


def check_column_length_min(records: list[dict], column: str, min_len: int) -> dict:
    """Check that all non-null string values have length >= min_len."""
    violations = []
    checked = 0
    for r in records:
        val = r.get(column)
        if val is None or val == "":
            continue
        checked += 1
        if len(str(val)) < min_len:
            violations.append({"record_id": r.get("record_id"), "value": str(val)[:50], "length": len(str(val))})
    return {
        "rule": f'ColumnLength "{column}" >= {min_len}',
        "type": "ColumnLength",
        "column": column,
        "min_length": min_len,
        "records_checked": checked,
        "violations": len(violations),
        "violation_details": violations[:10],
        "passed": len(violations) == 0,
    }


def check_regex_match(records: list[dict], column: str, pattern_str: str, threshold: float = 1.0) -> dict:
    """Check that column values match a regex pattern (with optional threshold)."""
    pattern = re.compile(pattern_str)
    total = 0
    matched = 0
    violations = []
    for r in records:
        val = r.get(column)
        if val is None or val == "":
            continue
        total += 1
        if pattern.match(str(val)):
            matched += 1
        else:
            violations.append({"record_id": r.get("record_id"), "value": str(val)[:80]})
    ratio = matched / total if total > 0 else 0
    return {
        "rule": f'ColumnValues "{column}" matches "{pattern_str}" (threshold >= {threshold})',
        "type": "RegexMatch",
        "column": column,
        "total_checked": total,
        "matched": matched,
        "ratio": round(ratio, 4),
        "violations": violations[:10],
        "passed": ratio >= threshold,
    }


def check_array_not_empty(records: list[dict], column: str, max_empty: int = 0) -> dict:
    """Check that an array field is not empty (simulates size() > 0 in Spark SQL)."""
    empty_count = 0
    for r in records:
        arr = r.get(column)
        if arr is not None and len(arr) == 0:
            empty_count += 1
    return {
        "rule": f'CustomSql: size({column}) = 0 count <= {max_empty}',
        "type": "ArrayNotEmpty",
        "column": column,
        "empty_count": empty_count,
        "max_allowed": max_empty,
        "passed": empty_count <= max_empty,
    }


def check_nested_vitals_range(records: list[dict], field: str, min_val, max_val) -> dict:
    """Check nested vitals_at_discharge struct field ranges."""
    violations = []
    for r in records:
        vitals = r.get("vitals_at_discharge", {})
        if vitals is None:
            continue
        val = vitals.get(field)
        if val is None:
            continue
        if val < min_val or val > max_val:
            violations.append({"record_id": r.get("record_id"), "value": val})
    return {
        "rule": f'CustomSql: vitals_at_discharge.{field} between {min_val} and {max_val}',
        "type": "NestedRange",
        "field": f"vitals_at_discharge.{field}",
        "violations": len(violations),
        "violation_details": violations[:10],
        "passed": len(violations) == 0,
    }


# ═══════════════════════════════════════════════════════════════════════════════
# LOCAL SIMULATION — Run rules for each table type
# ═══════════════════════════════════════════════════════════════════════════════

def run_csv_rules(records: list[dict]) -> dict:
    """Execute CSV table DQ rules locally and return a quality report."""
    results = []

    # Completeness checks
    for col, threshold in [("patient_name", 0.98), ("mrn", 1.0),
                            ("admission_date", 1.0), ("discharge_date", 1.0),
                            ("primary_icd10", 0.95), ("systolic_bp", 0.90),
                            ("diastolic_bp", 0.90)]:
        results.append(check_completeness(records, col, threshold))

    # Uniqueness
    results.append(check_uniqueness(records, "record_id"))

    # Consistency
    results.append(check_date_consistency(records))

    # Range checks
    for col, lo, hi in [("age", 0, 120), ("systolic_bp", 50, 250),
                         ("diastolic_bp", 30, 150), ("heart_rate", 20, 250),
                         ("temperature_f", 90.0, 108.0),
                         ("length_of_stay_days", 0, 100)]:
        results.append(check_column_range(records, col, lo, hi))

    # Format checks
    results.append(check_icd10_format(records))

    # Statistical checks
    ages = [r["age"] for r in records if r.get("age") is not None]
    results.append({
        "rule": 'Mean "age" between 40 and 80',
        "type": "Statistical",
        "actual": round(statistics.mean(ages), 2) if ages else None,
        "passed": 40 <= statistics.mean(ages) <= 80 if ages else False,
    })

    los = [r["length_of_stay_days"] for r in records if r.get("length_of_stay_days") is not None]
    results.append({
        "rule": 'StandardDeviation "length_of_stay_days" <= 15',
        "type": "Statistical",
        "actual": round(statistics.stdev(los), 2) if len(los) > 1 else None,
        "passed": statistics.stdev(los) <= 15 if len(los) > 1 else False,
    })

    # Row count
    results.append({
        "rule": "RowCount >= 100",
        "type": "RowCount",
        "actual": len(records),
        "passed": len(records) >= 100,
    })

    passed = sum(1 for r in results if r["passed"])
    total = len(results)
    return {
        "report_timestamp": datetime.now().isoformat(),
        "dataset": "patient_records.csv",
        "table_name": "patient_records_csv",
        "total_records": len(records),
        "total_rules": total,
        "rules_passed": passed,
        "rules_failed": total - passed,
        "overall_score": round(passed / total * 100, 1),
        "rule_results": results,
    }


def run_json_rules(records: list[dict]) -> dict:
    """Execute JSON table DQ rules locally (simulates nested data checks)."""
    results = []

    # Completeness — top-level fields
    for col, threshold in [("patient_name", 0.98), ("mrn", 1.0), ("record_id", 1.0),
                            ("admission_date", 1.0), ("discharge_date", 1.0),
                            ("attending_physician", 1.0), ("department", 1.0)]:
        results.append(check_completeness(records, col, threshold))

    # Null checks for nested fields (replaces Completeness which doesn't work on array/struct)
    for col, max_null in [("diagnoses", 10), ("medications", 20), ("vitals_at_discharge", 20)]:
        null_count = sum(1 for r in records if r.get(col) is None)
        results.append({
            "rule": f'CustomSql: {col} IS NULL count <= {max_null}',
            "type": "NullCheck",
            "column": col,
            "null_count": null_count,
            "max_allowed": max_null,
            "passed": null_count <= max_null,
        })

    # Uniqueness
    results.append(check_uniqueness(records, "record_id"))

    # Range checks
    results.append(check_column_range(records, "age", 0, 120))
    results.append(check_column_range(records, "length_of_stay_days", 0, 100))

    # Value checks
    results.append(check_column_values_in(records, "gender", ["M", "F"]))

    # Consistency
    results.append(check_date_consistency(records))

    # Array checks — diagnoses should not be empty
    results.append(check_array_not_empty(records, "diagnoses", max_empty=0))

    # Array checks — medications (allow up to 10 empty)
    results.append(check_array_not_empty(records, "medications", max_empty=10))

    # Nested vitals checks
    results.append(check_nested_vitals_range(records, "systolic_bp", 50, 250))
    results.append(check_nested_vitals_range(records, "heart_rate", 0, 250))
    results.append(check_nested_vitals_range(records, "temperature_f", 90.0, 108.0))

    # Statistical checks
    ages = [r["age"] for r in records if r.get("age") is not None]
    results.append({
        "rule": 'Mean "age" between 40 and 80',
        "type": "Statistical",
        "actual": round(statistics.mean(ages), 2) if ages else None,
        "passed": 40 <= statistics.mean(ages) <= 80 if ages else False,
    })

    los = [r["length_of_stay_days"] for r in records if r.get("length_of_stay_days") is not None]
    results.append({
        "rule": 'StandardDeviation "length_of_stay_days" <= 15',
        "type": "Statistical",
        "actual": round(statistics.stdev(los), 2) if len(los) > 1 else None,
        "passed": statistics.stdev(los) <= 15 if len(los) > 1 else False,
    })

    # Row count
    results.append({
        "rule": "RowCount >= 100",
        "type": "RowCount",
        "actual": len(records),
        "passed": len(records) >= 100,
    })

    passed = sum(1 for r in results if r["passed"])
    total = len(results)
    return {
        "report_timestamp": datetime.now().isoformat(),
        "dataset": "patient_records.json",
        "table_name": "patient_records_json",
        "total_records": len(records),
        "total_rules": total,
        "rules_passed": passed,
        "rules_failed": total - passed,
        "overall_score": round(passed / total * 100, 1),
        "rule_results": results,
    }


def run_notes_rules(notes: list[dict]) -> dict:
    """Execute clinical notes DQ rules locally."""
    results = []

    # Completeness — all fields required
    for col in ["note_text", "record_id", "mrn", "note_type", "author", "date"]:
        results.append(check_completeness(notes, col, 1.0))

    # Uniqueness
    results.append(check_uniqueness(notes, "record_id"))

    # Value checks
    results.append(check_column_values_in(
        notes, "note_type", ["Discharge Summary", "Progress Note", "H&P"]))

    # Text quality
    results.append(check_column_length_min(notes, "note_text", 20))

    # Format checks
    results.append(check_regex_match(
        notes, "date", r"^\d{4}-\d{2}-\d{2}$", threshold=0.95))
    results.append(check_regex_match(
        notes, "mrn", r"^MRN-\d{6}$", threshold=0.95))
    results.append(check_regex_match(
        notes, "author", r"^Dr\. [A-Z][a-z]+$", threshold=0.90))

    # Row count
    results.append({
        "rule": "RowCount >= 100",
        "type": "RowCount",
        "actual": len(notes),
        "passed": len(notes) >= 100,
    })
    results.append({
        "rule": "RowCount = 200",
        "type": "RowCount",
        "actual": len(notes),
        "passed": len(notes) == 200,
    })

    passed = sum(1 for r in results if r["passed"])
    total = len(results)
    return {
        "report_timestamp": datetime.now().isoformat(),
        "dataset": "clinical_notes.json",
        "table_name": "clinical_notes_json",
        "total_records": len(notes),
        "total_rules": total,
        "rules_passed": passed,
        "rules_failed": total - passed,
        "overall_score": round(passed / total * 100, 1),
        "rule_results": results,
    }


# Keep backward-compatible name
run_all_rules = run_csv_rules


# ═══════════════════════════════════════════════════════════════════════════════
# API EXAMPLES
# ═══════════════════════════════════════════════════════════════════════════════

def generate_glue_dq_api_example() -> dict:
    """Generate AWS API call structures for all 3 rulesets."""
    tables = [
        {
            "ruleset_name": "healthcare-patient-records-csv-dq",
            "table_name": "patient_records_csv",
            "description": "Data quality rules for patient discharge records (CSV)",
            "dqdl_var": "GLUE_DQ_RULESET_CSV",
        },
        {
            "ruleset_name": "healthcare-patient-records-json-dq",
            "table_name": "patient_records_json",
            "description": "Data quality rules for patient records with nested JSON (arrays, structs)",
            "dqdl_var": "GLUE_DQ_RULESET_JSON",
        },
        {
            "ruleset_name": "healthcare-clinical-notes-json-dq",
            "table_name": "clinical_notes_json",
            "description": "Data quality rules for clinical text notes",
            "dqdl_var": "GLUE_DQ_RULESET_NOTES",
        },
    ]

    examples = []
    for t in tables:
        examples.append({
            "api_call": "glue.create_data_quality_ruleset",
            "parameters": {
                "Name": t["ruleset_name"],
                "Description": t["description"],
                "Ruleset": f"<see {t['dqdl_var']} in code>",
                "TargetTable": {
                    "TableName": t["table_name"],
                    "DatabaseName": "demo_1_healthcare_records_pdx",
                },
            },
            "run_api_call": "glue.start_data_quality_ruleset_evaluation_run",
            "run_parameters": {
                "DataSource": {
                    "GlueTable": {
                        "TableName": t["table_name"],
                        "DatabaseName": "demo_1_healthcare_records_pdx",
                    }
                },
                "RulesetNames": [t["ruleset_name"]],
                "Role": "arn:aws:iam::ACCOUNT_ID:role/AWSGlueServiceRole-HealthRecords",
            },
        })

    return {
        "rulesets": examples,
        "note": "JSON tables MUST be in JSON Lines format (one object per line). "
                "JSON arrays produce a single 'array' column that breaks DQDL rules. "
                "See AWS_AI_LEARNINGS.md entry #14.",
    }


# ═══════════════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════════════

def print_report(report: dict) -> None:
    """Print a formatted quality report."""
    print(f"\n{'='*70}")
    print(f"  TABLE: {report['table_name']}  ({report['dataset']})")
    print(f"{'='*70}")
    print(f"  Total records:  {report['total_records']}")
    print(f"  Total rules:    {report['total_rules']}")
    print(f"  Rules passed:   {report['rules_passed']}")
    print(f"  Rules failed:   {report['rules_failed']}")
    print(f"  Overall score:  {report['overall_score']}%")
    print(f"{'─'*70}")

    for result in report["rule_results"]:
        status = "✅ PASS" if result["passed"] else "❌ FAIL"
        print(f"  {status}  {result['rule']}")


def main():
    csv_path = DATA_DIR / "patient_records.csv"
    json_path = DATA_DIR / "patient_records.json"
    notes_path = DATA_DIR / "clinical_notes.json"

    for path in [csv_path, json_path, notes_path]:
        if not path.exists():
            print(f"ERROR: {path} not found. Run generate_healthcare_data.py first.")
            return

    # ── Run CSV rules ──
    print("Loading patient records (CSV)...")
    csv_records = load_csv_records(str(csv_path))
    print(f"  Loaded {len(csv_records)} records")

    csv_report = run_csv_rules(csv_records)
    print_report(csv_report)

    # ── Run JSON rules ──
    print("\nLoading patient records (JSON)...")
    json_records = load_json_records(str(json_path))
    print(f"  Loaded {len(json_records)} records")

    json_report = run_json_rules(json_records)
    print_report(json_report)

    # ── Run clinical notes rules ──
    print("\nLoading clinical notes...")
    notes = load_json_records(str(notes_path))
    print(f"  Loaded {len(notes)} notes")

    notes_report = run_notes_rules(notes)
    print_report(notes_report)

    # ── Save all outputs ──
    # Combined report
    all_reports = {
        "report_timestamp": datetime.now().isoformat(),
        "tables": {
            "patient_records_csv": csv_report,
            "patient_records_json": json_report,
            "clinical_notes_json": notes_report,
        },
        "summary": {
            "total_tables": 3,
            "total_rules": csv_report["total_rules"] + json_report["total_rules"] + notes_report["total_rules"],
            "total_passed": csv_report["rules_passed"] + json_report["rules_passed"] + notes_report["rules_passed"],
            "total_failed": csv_report["rules_failed"] + json_report["rules_failed"] + notes_report["rules_failed"],
        },
    }
    total_r = all_reports["summary"]["total_rules"]
    total_p = all_reports["summary"]["total_passed"]
    all_reports["summary"]["overall_score"] = round(total_p / total_r * 100, 1) if total_r > 0 else 0

    # Save combined report
    with open(OUTPUT_DIR / "glue_dq_report.json", "w") as f:
        json.dump(all_reports, f, indent=2, default=str)

    # Save individual DQDL rulesets
    rulesets = {
        "glue_dq_ruleset.dqdl": GLUE_DQ_RULESET_CSV,  # backward compat
        "glue_dq_ruleset_patient_records_csv.dqdl": GLUE_DQ_RULESET_CSV,
        "glue_dq_ruleset_patient_records_json.dqdl": GLUE_DQ_RULESET_JSON,
        "glue_dq_ruleset_clinical_notes_json.dqdl": GLUE_DQ_RULESET_NOTES,
    }
    for filename, content in rulesets.items():
        with open(OUTPUT_DIR / filename, "w") as f:
            f.write(content)

    # Save API example
    with open(OUTPUT_DIR / "glue_dq_api_example.json", "w") as f:
        json.dump(generate_glue_dq_api_example(), f, indent=2)

    # ── Final summary ──
    print(f"\n{'='*70}")
    print(f"  COMBINED QUALITY SUMMARY (ALL 3 TABLES)")
    print(f"{'='*70}")
    print(f"  Tables evaluated:  3")
    print(f"  Total rules:       {all_reports['summary']['total_rules']}")
    print(f"  Total passed:      {all_reports['summary']['total_passed']}")
    print(f"  Total failed:      {all_reports['summary']['total_failed']}")
    print(f"  Combined score:    {all_reports['summary']['overall_score']}%")
    print(f"{'='*70}")
    print(f"\nReports saved to: {OUTPUT_DIR}")
    print(f"  - glue_dq_report.json (combined report for all 3 tables)")
    print(f"  - glue_dq_ruleset_patient_records_csv.dqdl")
    print(f"  - glue_dq_ruleset_patient_records_json.dqdl")
    print(f"  - glue_dq_ruleset_clinical_notes_json.dqdl")
    print(f"  - glue_dq_api_example.json")

    return all_reports


if __name__ == "__main__":
    main()
