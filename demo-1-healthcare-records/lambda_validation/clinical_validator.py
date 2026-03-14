"""Custom Lambda function for domain-specific clinical data validation.

Demonstrates:
  - ICD-10 code validation against a reference set
  - Vital sign physiological plausibility checks
  - Medication interaction cross-referencing
  - Date logic and business rule enforcement
  - Structured validation results for downstream processing
  - **CloudWatch metrics publishing** (dual-mode: AWS API + local file fallback)
  - **Responsible AI pre-FM gates** (profanity, PHI exposure, clinical relevance,
    bias indicators, content coherence)

Covers requirement 1.3: Custom Lambda functions for specialized validation logic
Covers requirement 1.4: CloudWatch metrics for quality monitoring
Covers requirement 1.5: Validation for both structured and unstructured data
Covers requirement 3.1: Responsible AI checks as pre-FM data gates
"""

import json
import re
from datetime import datetime, timezone
from pathlib import Path

OUTPUT_DIR = Path(__file__).parent / "output"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
DATA_DIR = Path(__file__).parent.parent / "synth_data" / "output"

# CloudWatch namespace — MUST match quality_dashboard.py
CW_NAMESPACE = "AIP-C01/HealthcareDataQuality"

# ---------------------------------------------------------------------------
# Domain-specific reference data
# ---------------------------------------------------------------------------
VALID_ICD10_PATTERN = re.compile(r"^[A-Z]\d{2}(\.\d{1,4})?$")

# Known drug interactions (simplified for demo)
DRUG_INTERACTIONS = {
    frozenset({"Warfarin", "Aspirin"}): {
        "severity": "HIGH",
        "description": "Increased bleeding risk with concurrent anticoagulant and antiplatelet therapy",
    },
    frozenset({"Lisinopril", "Furosemide"}): {
        "severity": "MODERATE",
        "description": "ACE inhibitor + loop diuretic may cause excessive hypotension",
    },
    frozenset({"Metformin", "Furosemide"}): {
        "severity": "LOW",
        "description": "Loop diuretics may increase blood glucose, reducing metformin efficacy",
    },
}

# Vital sign ranges (clinical plausibility, not just database constraints)
VITAL_RANGES = {
    "systolic_bp":       {"min": 60,  "max": 250, "critical_low": 70,  "critical_high": 200},
    "diastolic_bp":      {"min": 30,  "max": 150, "critical_low": 40,  "critical_high": 120},
    "heart_rate":        {"min": 30,  "max": 220, "critical_low": 40,  "critical_high": 180},
    "temperature_f":     {"min": 90,  "max": 108, "critical_low": 95,  "critical_high": 104},
    "respiratory_rate":  {"min": 6,   "max": 60,  "critical_low": 8,   "critical_high": 40},
    "oxygen_saturation": {"min": 50,  "max": 100, "critical_low": 88,  "critical_high": 100},
}

# ---------------------------------------------------------------------------
# Responsible AI — reference data for pre-FM gate checks
# ---------------------------------------------------------------------------

# Healthcare-appropriate profanity/slur blocklist (representative, not exhaustive)
# In production, use a maintained blocklist service or Amazon Comprehend toxicity detection
_PROFANITY_PATTERNS = re.compile(
    r"\b("
    r"damn|shit|fuck|bitch|bastard|asshole|crap|"
    r"retard(?:ed)?|idiot|moron|stupid\s+patient|"
    r"junkie|addict\s+scum|druggie|drunk(?:ard)?"
    r")\b",
    re.IGNORECASE,
)

# PHI patterns that should NOT appear in clinical notes sent to an FM
_SSN_PATTERN = re.compile(r"\b\d{3}-\d{2}-\d{4}\b")
_EMAIL_PATTERN = re.compile(r"[\w.+-]+@[\w-]+\.[\w.-]+")
_PHONE_PATTERN = re.compile(
    r"\b(?:\+?1[-.\s]?)?\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}\b"
)
_ADDRESS_PATTERN = re.compile(
    r"\b\d{1,5}\s+(?:[A-Z][a-z]+\s+){1,3}"
    r"(?:St(?:reet)?|Ave(?:nue)?|Blvd|Dr(?:ive)?|Rd|Ln|Ct|Way|Pl)\b",
    re.IGNORECASE,
)

# Medical terminology — presence indicates clinical relevance
_MEDICAL_TERMS = re.compile(
    r"\b("
    r"diagnosis|prognosis|treatment|therapy|medication|prescription|dosage|"
    r"symptom|sign|assessment|plan|follow[- ]?up|discharge|admission|"
    r"vital|blood\s+pressure|heart\s+rate|temperature|respiratory|"
    r"oxygen|saturation|CBC|BMP|CMP|CT|MRI|X-ray|ultrasound|"
    r"surgery|procedure|biopsy|pathology|lab|culture|"
    r"hypertension|diabetes|pneumonia|fracture|infection|"
    r"antibiotic|analgesic|insulin|metformin|aspirin|warfarin|"
    r"patient|chief\s+complaint|history|physical\s+exam|"
    r"mg|mcg|ml|BID|QD|TID|QHS|PRN"
    r")\b",
    re.IGNORECASE,
)

# Bias indicator patterns — demographic-loaded language that could skew FM reasoning
_BIAS_INDICATORS = re.compile(
    r"\b("
    r"non-?compliant\s+(?:elderly|older|young)|"
    r"drug[- ]?seeking\s+behavio(?:u)?r|"
    r"frequent\s+flyer|"
    r"poor\s+historian|"
    r"(?:un)?reliable\s+(?:historian|reporter)|"
    r"difficult\s+patient|"
    r"(?:male|female)\s+(?:hysteri(?:a|cal))|"
    r"(?:she|he)\s+(?:claims|alleges)\s+(?:pain|symptoms)|"
    r"(?:elderly|geriatric)\s+(?:and\s+)?(?:confused|demented)|"
    r"(?:obese|overweight)\s+(?:and\s+)?(?:non-?compliant|lazy)"
    r")\b",
    re.IGNORECASE,
)

# Clinical note structural markers
_CLINICAL_SECTIONS = re.compile(
    r"\b("
    r"Assessment|Plan|Subjective|Objective|"
    r"Chief\s+Complaint|History|Physical\s+Exam|"
    r"Hospital\s+Course|Discharge\s+(?:Summary|Instructions)|"
    r"Follow[- ]?up|Medications|Diagnos[ei]s"
    r")[\s:—]",
    re.IGNORECASE,
)


# ---------------------------------------------------------------------------
# Responsible AI checks
# ---------------------------------------------------------------------------
def responsible_ai_checks(record: dict, clinical_note: str) -> list[dict]:
    """Run Responsible AI pre-FM gate checks on a clinical record and its note.

    These are basic pattern-matching checks designed to catch obvious problems
    BEFORE data reaches a foundation model (e.g., Bedrock). They are NOT a
    substitute for ML-based content moderation (e.g., Amazon Comprehend toxicity
    detection or Bedrock Guardrails).

    Args:
        record: Patient record dict (for context like record_id).
        clinical_note: The free-text clinical note to evaluate.

    Returns:
        List of issue dicts, each with type/severity/message keys.
    """
    issues: list[dict] = []
    record_id = record.get("record_id", "unknown")

    if not clinical_note or not clinical_note.strip():
        issues.append({
            "type": "RA_EMPTY_NOTE",
            "field": "clinical_note",
            "severity": "WARNING",
            "message": "Clinical note is empty — no content to send to FM",
        })
        return issues  # No further checks possible

    # --- (a) Profanity / Inappropriate Language Filter ---
    profanity_matches = _PROFANITY_PATTERNS.findall(clinical_note)
    if profanity_matches:
        issues.append({
            "type": "RA_PROFANITY",
            "field": "clinical_note",
            "severity": "ERROR",
            "value": profanity_matches[:3],  # Limit exposure in logs
            "message": (
                f"Profanity/inappropriate language detected in note for record "
                f"{record_id}: {len(profanity_matches)} occurrence(s). "
                f"Record BLOCKED from FM consumption."
            ),
        })

    # --- (b) PHI Exposure Risk Assessment ---
    phi_findings: list[str] = []
    if _SSN_PATTERN.search(clinical_note):
        phi_findings.append("SSN")
    if _EMAIL_PATTERN.search(clinical_note):
        phi_findings.append("email")
    if _PHONE_PATTERN.search(clinical_note):
        phi_findings.append("phone_number")
    if _ADDRESS_PATTERN.search(clinical_note):
        phi_findings.append("street_address")
    if phi_findings:
        issues.append({
            "type": "RA_PHI_EXPOSURE",
            "field": "clinical_note",
            "severity": "WARNING",
            "value": phi_findings,
            "message": (
                f"Potential PHI detected in note for record {record_id}: "
                f"{', '.join(phi_findings)}. Route through Comprehend Medical "
                f"de-identification before FM consumption."
            ),
        })

    # --- (c) Clinical Relevance Check ---
    words = clinical_note.split()
    word_count = len(words)

    # Too short to be clinically useful
    if word_count < 20:
        issues.append({
            "type": "RA_LOW_RELEVANCE",
            "field": "clinical_note",
            "severity": "WARNING",
            "value": word_count,
            "message": (
                f"Clinical note for record {record_id} has only {word_count} words "
                f"(minimum 20 recommended). May be too short for useful FM analysis."
            ),
        })

    # Check for medical terminology presence
    med_term_matches = _MEDICAL_TERMS.findall(clinical_note)
    med_term_ratio = len(set(med_term_matches)) / max(word_count, 1)
    if not med_term_matches:
        issues.append({
            "type": "RA_LOW_RELEVANCE",
            "field": "clinical_note",
            "severity": "WARNING",
            "message": (
                f"Clinical note for record {record_id} contains no recognized "
                f"medical terminology. May not be a valid clinical note."
            ),
        })

    # Gibberish / garbled OCR detection — high ratio of non-alphabetic characters
    alpha_chars = sum(1 for c in clinical_note if c.isalpha() or c.isspace())
    total_chars = len(clinical_note)
    if total_chars > 0:
        alpha_ratio = alpha_chars / total_chars
        if alpha_ratio < 0.60:
            issues.append({
                "type": "RA_GARBLED_TEXT",
                "field": "clinical_note",
                "severity": "WARNING",
                "value": round(alpha_ratio, 3),
                "message": (
                    f"Clinical note for record {record_id} has low alphabetic ratio "
                    f"({alpha_ratio:.1%}). Possible garbled OCR or data corruption."
                ),
            })

    # --- (d) Bias Indicator Detection ---
    bias_matches = _BIAS_INDICATORS.findall(clinical_note)
    if bias_matches:
        issues.append({
            "type": "RA_BIAS_INDICATOR",
            "field": "clinical_note",
            "severity": "WARNING",
            "value": bias_matches[:3],
            "message": (
                f"Bias-loaded language detected in note for record {record_id}: "
                f"{bias_matches[:3]}. This phrasing may inappropriately influence "
                f"FM reasoning. Consider neutral clinical language."
            ),
        })

    # --- (e) Content Coherence Check ---
    # At least one complete sentence (ends with period, question mark, or exclamation)
    sentences = re.split(r"[.!?]+", clinical_note.strip())
    meaningful_sentences = [s.strip() for s in sentences if len(s.strip().split()) >= 3]
    if not meaningful_sentences:
        issues.append({
            "type": "RA_LOW_COHERENCE",
            "field": "clinical_note",
            "severity": "WARNING",
            "message": (
                f"Clinical note for record {record_id} has no complete sentences. "
                f"May be a fragment, keyword list, or data artifact."
            ),
        })

    # Structured section detection (bonus signal, not a hard fail)
    has_sections = bool(_CLINICAL_SECTIONS.search(clinical_note))
    if not has_sections and word_count >= 40:
        issues.append({
            "type": "RA_UNSTRUCTURED_NOTE",
            "field": "clinical_note",
            "severity": "WARNING",
            "message": (
                f"Clinical note for record {record_id} ({word_count} words) lacks "
                f"standard clinical sections (Assessment, Plan, etc.). Structured "
                f"notes produce better FM outputs."
            ),
        })

    return issues


def compute_clinical_relevance_score(clinical_note: str) -> float:
    """Compute a 0–100 clinical relevance score for a note.

    Factors:
      - Word count (longer is better up to a point)
      - Medical terminology density
      - Structural markers
      - Sentence completeness

    Args:
        clinical_note: The free-text clinical note.

    Returns:
        Score from 0.0 to 100.0.
    """
    if not clinical_note or not clinical_note.strip():
        return 0.0

    score = 0.0
    words = clinical_note.split()
    word_count = len(words)

    # Word count score (0–30 points): 0 at 0 words, max at 50+ words
    score += min(word_count / 50.0, 1.0) * 30.0

    # Medical terminology density (0–30 points)
    med_terms = set(_MEDICAL_TERMS.findall(clinical_note))
    term_density = len(med_terms) / max(word_count, 1)
    score += min(term_density * 10, 1.0) * 30.0

    # Structural markers (0–20 points)
    section_matches = _CLINICAL_SECTIONS.findall(clinical_note)
    score += min(len(section_matches) / 3.0, 1.0) * 20.0

    # Sentence completeness (0–20 points)
    sentences = re.split(r"[.!?]+", clinical_note.strip())
    complete = [s for s in sentences if len(s.strip().split()) >= 3]
    if sentences:
        score += (len(complete) / max(len(sentences), 1)) * 20.0

    return round(min(score, 100.0), 1)


# ---------------------------------------------------------------------------
# Lambda handler (can be deployed as-is to AWS Lambda)
# ---------------------------------------------------------------------------
def lambda_handler(event, context=None):
    """Validate a batch of patient records with clinical domain logic.

    Event structure:
    {
        "records": [...],              # List of patient record dicts
        "validation_level": "full",    # "basic" | "full" | "strict"
        "emit_cloudwatch": false,      # Publish metrics to CloudWatch?
        "clinical_notes": {            # Optional: {record_id: note_text} map
            "1": "Patient presented...",
            ...
        }
    }

    Returns:
    {
        "validated_records": [...],
        "validation_summary": {...},
        "flagged_records": [...],
        "responsible_ai_summary": {...},
        "metrics_published": bool
    }
    """
    records = event.get("records", [])
    level = event.get("validation_level", "full")
    emit_cw = event.get("emit_cloudwatch", False)
    notes_map = event.get("clinical_notes", {})

    validated = []
    flagged = []
    stats = {
        "total": len(records),
        "passed": 0,
        "failed": 0,
        "warnings": 0,
        "errors_by_type": {},
    }
    ra_stats = {
        "records_checked": 0,
        "profanity_detected": 0,
        "phi_exposure_risk": 0,
        "low_relevance": 0,
        "bias_indicators": 0,
        "low_coherence": 0,
        "garbled_text": 0,
        "unstructured_notes": 0,
        "empty_notes": 0,
        "total_ra_issues": 0,
        "clinical_relevance_scores": [],
    }

    for record in records:
        rid = record.get("record_id")
        result = validate_record(record, level)
        result["record_id"] = rid

        # --- Responsible AI checks (run on "full" and "strict" levels) ---
        if level in ("full", "strict"):
            # Look up clinical note: in event map, or inline on record
            note_text = notes_map.get(str(rid), "") or record.get("clinical_note", "")
            ra_issues = responsible_ai_checks(record, note_text)
            result["issues"].extend(ra_issues)
            result["responsible_ai_issues"] = ra_issues

            # Compute relevance score
            relevance_score = compute_clinical_relevance_score(note_text)
            result["clinical_relevance_score"] = relevance_score
            ra_stats["clinical_relevance_scores"].append(relevance_score)

            # Update RA stats
            ra_stats["records_checked"] += 1
            for issue in ra_issues:
                ra_stats["total_ra_issues"] += 1
                itype = issue["type"]
                if itype == "RA_PROFANITY":
                    ra_stats["profanity_detected"] += 1
                elif itype == "RA_PHI_EXPOSURE":
                    ra_stats["phi_exposure_risk"] += 1
                elif itype == "RA_LOW_RELEVANCE":
                    ra_stats["low_relevance"] += 1
                elif itype == "RA_BIAS_INDICATOR":
                    ra_stats["bias_indicators"] += 1
                elif itype == "RA_LOW_COHERENCE":
                    ra_stats["low_coherence"] += 1
                elif itype == "RA_GARBLED_TEXT":
                    ra_stats["garbled_text"] += 1
                elif itype == "RA_UNSTRUCTURED_NOTE":
                    ra_stats["unstructured_notes"] += 1
                elif itype == "RA_EMPTY_NOTE":
                    ra_stats["empty_notes"] += 1

            # Re-count errors/warnings after RA issues added
            ra_errors = [i for i in ra_issues if i["severity"] == "ERROR"]
            ra_warnings = [i for i in ra_issues if i["severity"] == "WARNING"]
            result["error_count"] += len(ra_errors)
            result["warning_count"] += len(ra_warnings)

            # Recompute status if RA checks introduced new errors
            if ra_errors and result["status"] != "FAIL":
                result["status"] = "FAIL"
            elif ra_warnings and result["status"] == "PASS":
                result["status"] = "WARNING"

        # Classify into passed / warned / failed
        if result["status"] == "PASS":
            stats["passed"] += 1
            validated.append({**record, "_validation": result})
        elif result["status"] == "WARNING":
            stats["warnings"] += 1
            validated.append({**record, "_validation": result})
            flagged.append(result)
        else:
            stats["failed"] += 1
            flagged.append(result)

        # Track error types (including RA types)
        for issue in result.get("issues", []):
            etype = issue["type"]
            stats["errors_by_type"][etype] = stats["errors_by_type"].get(etype, 0) + 1

    # Compute average clinical relevance score
    scores = ra_stats["clinical_relevance_scores"]
    ra_stats["avg_clinical_relevance_score"] = (
        round(sum(scores) / len(scores), 1) if scores else 0.0
    )

    # Compute overall quality score (like reference sample)
    stats["quality_score"] = round(
        (stats["passed"] / max(stats["total"], 1)) * 100, 1
    )

    # --- Publish CloudWatch metrics ---
    metrics_published = False
    all_metrics = build_metric_data(stats, ra_stats)
    if emit_cw:
        metrics_published = publish_metrics(all_metrics, use_cloudwatch=True)

    # Build RA summary (strip large list for response)
    ra_summary = {k: v for k, v in ra_stats.items() if k != "clinical_relevance_scores"}

    return {
        "validated_records": validated,
        "validation_summary": stats,
        "flagged_records": flagged,
        "responsible_ai_summary": ra_summary,
        "metrics_published": metrics_published,
    }


def validate_record(record: dict, level: str = "full") -> dict:
    """Apply all validation rules to a single patient record."""
    issues = []

    # --- BASIC VALIDATIONS (always run) ---

    # 1. Required field presence
    required_fields = ["mrn", "patient_name", "age", "gender",
                       "admission_date", "discharge_date", "diagnoses"]
    for field in required_fields:
        val = record.get(field)
        if val is None or val == "" or val == []:
            issues.append({
                "type": "MISSING_REQUIRED_FIELD",
                "field": field,
                "severity": "ERROR",
                "message": f"Required field '{field}' is missing or empty",
            })

    # 2. Age validation
    age = record.get("age")
    if age is not None:
        if not isinstance(age, (int, float)) or age < 0 or age > 120:
            issues.append({
                "type": "INVALID_AGE",
                "field": "age",
                "severity": "ERROR",
                "value": age,
                "message": f"Age {age} is outside valid range [0, 120]",
            })

    # 3. Gender validation
    gender = record.get("gender", "")
    if gender not in ["M", "F", "O", "U", ""]:
        issues.append({
            "type": "INVALID_GENDER",
            "field": "gender",
            "severity": "WARNING",
            "value": gender,
            "message": f"Gender '{gender}' not in standard set (M/F/O/U)",
        })

    # 4. Date validation
    issues.extend(validate_dates(record))

    if level in ("full", "strict"):
        # --- FULL VALIDATIONS ---

        # 5. ICD-10 code validation
        for dx in record.get("diagnoses", []):
            code = dx.get("icd10", "")
            if code and not VALID_ICD10_PATTERN.match(code):
                issues.append({
                    "type": "INVALID_ICD10",
                    "field": "diagnoses.icd10",
                    "severity": "ERROR",
                    "value": code,
                    "message": f"ICD-10 code '{code}' does not match expected format",
                })

        # 6. Vital signs validation
        vitals = record.get("vitals_at_discharge", {})
        issues.extend(validate_vitals(vitals))

        # 7. Medication interaction check
        med_names = [m.get("name") for m in record.get("medications", []) if m.get("name")]
        issues.extend(check_drug_interactions(med_names))

        # NOTE: Responsible AI checks are called from lambda_handler after this
        # function returns, so they can access the clinical_note from the notes map.

    if level == "strict":
        # --- STRICT VALIDATIONS ---

        # 8. Clinical note presence check
        if not record.get("clinical_note"):
            issues.append({
                "type": "MISSING_CLINICAL_NOTE",
                "field": "clinical_note",
                "severity": "WARNING",
                "message": "No clinical note attached to record",
            })

        # 9. Medication dosage format validation
        for med in record.get("medications", []):
            dose = med.get("dose", "")
            if dose and not re.match(r"^\d+(\.\d+)?\s*(mg|mcg|ml|units?|g)$", dose, re.IGNORECASE):
                issues.append({
                    "type": "INVALID_DOSAGE_FORMAT",
                    "field": "medications.dose",
                    "severity": "WARNING",
                    "value": dose,
                    "message": f"Dosage '{dose}' may not be in standard format",
                })

    # Determine overall status
    errors = [i for i in issues if i["severity"] == "ERROR"]
    warnings = [i for i in issues if i["severity"] == "WARNING"]

    if errors:
        status = "FAIL"
    elif warnings:
        status = "WARNING"
    else:
        status = "PASS"

    return {
        "status": status,
        "issues": issues,
        "error_count": len(errors),
        "warning_count": len(warnings),
    }


def validate_dates(record: dict) -> list[dict]:
    """Validate date fields for logical consistency."""
    issues = []
    try:
        admit = datetime.strptime(record.get("admission_date", ""), "%Y-%m-%d")
        discharge = datetime.strptime(record.get("discharge_date", ""), "%Y-%m-%d")

        if discharge < admit:
            issues.append({
                "type": "DATE_INCONSISTENCY",
                "field": "discharge_date",
                "severity": "ERROR",
                "message": f"Discharge date ({record['discharge_date']}) before "
                           f"admission date ({record['admission_date']})",
            })

        if discharge > datetime.now():
            issues.append({
                "type": "FUTURE_DATE",
                "field": "discharge_date",
                "severity": "ERROR",
                "message": f"Discharge date ({record['discharge_date']}) is in the future",
            })

        los = record.get("length_of_stay_days")
        if los is not None:
            actual_los = (discharge - admit).days
            if abs(actual_los - los) > 1:
                issues.append({
                    "type": "LOS_MISMATCH",
                    "field": "length_of_stay_days",
                    "severity": "WARNING",
                    "message": f"Stated LOS ({los}) doesn't match computed ({actual_los})",
                })

    except (ValueError, TypeError):
        issues.append({
            "type": "INVALID_DATE_FORMAT",
            "severity": "ERROR",
            "message": "Could not parse admission or discharge date (expected YYYY-MM-DD)",
        })
    return issues


def validate_vitals(vitals: dict) -> list[dict]:
    """Validate vital signs against clinical plausibility ranges."""
    issues = []
    for vital_name, ranges in VITAL_RANGES.items():
        value = vitals.get(vital_name)
        if value is None:
            continue
        try:
            value = float(value)
        except (ValueError, TypeError):
            issues.append({
                "type": "INVALID_VITAL_VALUE",
                "field": f"vitals.{vital_name}",
                "severity": "ERROR",
                "value": vitals.get(vital_name),
                "message": f"Non-numeric vital sign value for {vital_name}",
            })
            continue

        if value < ranges["min"] or value > ranges["max"]:
            issues.append({
                "type": "VITAL_OUT_OF_RANGE",
                "field": f"vitals.{vital_name}",
                "severity": "ERROR",
                "value": value,
                "range": [ranges["min"], ranges["max"]],
                "message": f"{vital_name}={value} outside physiological range "
                           f"[{ranges['min']}, {ranges['max']}]",
            })
        elif value <= ranges["critical_low"] or value >= ranges["critical_high"]:
            issues.append({
                "type": "VITAL_CRITICAL",
                "field": f"vitals.{vital_name}",
                "severity": "WARNING",
                "value": value,
                "message": f"{vital_name}={value} is in critical range",
            })
    return issues


def check_drug_interactions(medication_names: list[str]) -> list[dict]:
    """Check for known drug-drug interactions."""
    issues = []
    med_set = set(medication_names)
    for drug_pair, interaction in DRUG_INTERACTIONS.items():
        if drug_pair.issubset(med_set):
            drugs = " + ".join(sorted(drug_pair))
            issues.append({
                "type": "DRUG_INTERACTION",
                "field": "medications",
                "severity": "WARNING" if interaction["severity"] != "HIGH" else "ERROR",
                "drugs": list(drug_pair),
                "interaction_severity": interaction["severity"],
                "message": f"Drug interaction ({interaction['severity']}): "
                           f"{drugs} — {interaction['description']}",
            })
    return issues


# ---------------------------------------------------------------------------
# CloudWatch metrics — build, publish, save
# ---------------------------------------------------------------------------
def build_metric_data(stats: dict, ra_stats: dict | None = None) -> list[dict]:
    """Convert validation + Responsible AI stats into CloudWatch PutMetricData format.

    Args:
        stats: Validation summary from lambda_handler (total, passed, failed, etc.)
        ra_stats: Responsible AI summary (profanity_detected, phi_exposure_risk, etc.)

    Returns:
        List of MetricData entries ready for cloudwatch.put_metric_data().
    """
    timestamp = datetime.now(timezone.utc).isoformat()
    metrics: list[dict] = []

    # --- Per-batch validation metrics ---
    pass_rate = (stats.get("passed", 0) / max(stats.get("total", 1), 1)) * 100
    quality_score = stats.get("quality_score", pass_rate)

    metrics.append({
        "MetricName": "Validation_PassRate",
        "Value": round(pass_rate, 1),
        "Unit": "Percent",
        "Timestamp": timestamp,
        "Dimensions": [{"Name": "Validator", "Value": "clinical-validator"}],
    })
    metrics.append({
        "MetricName": "Validation_ErrorCount",
        "Value": stats.get("failed", 0),
        "Unit": "Count",
        "Timestamp": timestamp,
        "Dimensions": [{"Name": "Validator", "Value": "clinical-validator"}],
    })
    metrics.append({
        "MetricName": "Validation_WarningCount",
        "Value": stats.get("warnings", 0),
        "Unit": "Count",
        "Timestamp": timestamp,
        "Dimensions": [{"Name": "Validator", "Value": "clinical-validator"}],
    })
    metrics.append({
        "MetricName": "Validation_QualityScore",
        "Value": round(quality_score, 1),
        "Unit": "Percent",
        "Timestamp": timestamp,
        "Dimensions": [{"Name": "Validator", "Value": "clinical-validator"}],
    })

    # --- Per-error-type metrics ---
    for error_type, count in stats.get("errors_by_type", {}).items():
        metrics.append({
            "MetricName": "Validation_ErrorsByType",
            "Value": count,
            "Unit": "Count",
            "Timestamp": timestamp,
            "Dimensions": [
                {"Name": "ErrorType", "Value": error_type},
                {"Name": "Validator", "Value": "clinical-validator"},
            ],
        })

    # --- Responsible AI metrics ---
    if ra_stats:
        metrics.append({
            "MetricName": "Validation_ProfanityDetected",
            "Value": ra_stats.get("profanity_detected", 0),
            "Unit": "Count",
            "Timestamp": timestamp,
            "Dimensions": [{"Name": "Validator", "Value": "clinical-validator"}],
        })
        metrics.append({
            "MetricName": "Validation_PHIExposureRisk",
            "Value": ra_stats.get("phi_exposure_risk", 0),
            "Unit": "Count",
            "Timestamp": timestamp,
            "Dimensions": [{"Name": "Validator", "Value": "clinical-validator"}],
        })
        avg_relevance = ra_stats.get("avg_clinical_relevance_score", 0.0)
        metrics.append({
            "MetricName": "Validation_ClinicalRelevanceScore",
            "Value": round(avg_relevance, 1),
            "Unit": "Percent",
            "Timestamp": timestamp,
            "Dimensions": [{"Name": "Validator", "Value": "clinical-validator"}],
        })

    return metrics


def publish_metrics(metrics: list[dict], use_cloudwatch: bool = False) -> bool:
    """Publish metrics to CloudWatch (if available) and always save locally.

    Args:
        metrics: List of CloudWatch MetricData dicts.
        use_cloudwatch: If True, attempt to publish via boto3.

    Returns:
        True if CloudWatch publish succeeded, False otherwise.
    """
    cw_success = False

    if use_cloudwatch:
        try:
            import boto3
            cw = boto3.client("cloudwatch")
            # CloudWatch accepts max 1000 data points per call; batch in groups of 20
            for i in range(0, len(metrics), 20):
                batch = metrics[i:i + 20]
                cw.put_metric_data(Namespace=CW_NAMESPACE, MetricData=batch)
            cw_success = True
            print(f"✓ Published {len(metrics)} metrics to CloudWatch namespace '{CW_NAMESPACE}'")
        except Exception as e:
            print(f"⚠ CloudWatch publish failed (running locally?): {e}")

    # Always save metrics locally
    save_metrics_to_file(metrics)
    print_metrics_summary(metrics)

    return cw_success


def save_metrics_to_file(metrics: list[dict]) -> Path:
    """Save metrics to a local JSON file for inspection and testing.

    Args:
        metrics: List of CloudWatch MetricData dicts.

    Returns:
        Path to the saved file.
    """
    output_path = OUTPUT_DIR / "cloudwatch_metrics.json"
    payload = {
        "namespace": CW_NAMESPACE,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "metric_count": len(metrics),
        "metrics": metrics,
    }
    with open(output_path, "w") as f:
        json.dump(payload, f, indent=2)
    return output_path


def print_metrics_summary(metrics: list[dict]) -> None:
    """Print a human-readable summary of metrics to stdout.

    Args:
        metrics: List of CloudWatch MetricData dicts.
    """
    print(f"\n{'─'*60}")
    print(f"CLOUDWATCH METRICS (namespace: {CW_NAMESPACE})")
    print(f"{'─'*60}")
    for m in metrics:
        dims = ", ".join(f"{d['Name']}={d['Value']}" for d in m.get("Dimensions", []))
        print(f"  {m['MetricName']:40s} = {m['Value']:>8} {m.get('Unit', ''):<10s}  [{dims}]")
    print(f"{'─'*60}")
    print(f"  Total metrics: {len(metrics)}")


# ---------------------------------------------------------------------------
# AWS Lambda deployment configuration
# ---------------------------------------------------------------------------
LAMBDA_DEPLOYMENT_CONFIG = {
    "FunctionName": "healthcare-clinical-validator",
    "Runtime": "python3.12",
    "Handler": "clinical_validator.lambda_handler",
    "MemorySize": 512,
    "Timeout": 300,
    "Environment": {
        "Variables": {
            "VALIDATION_LEVEL": "full",
            "LOG_LEVEL": "INFO",
        }
    },
    "Tags": {
        "Project": "aip-c01-demos",
        "Demo": "healthcare-records",
    },
    "Role": "arn:aws:iam::ACCOUNT_ID:role/healthcare-clinical-validator-role",
    "RolePolicyDocument": {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "CloudWatchMetrics",
                "Effect": "Allow",
                "Action": [
                    "cloudwatch:PutMetricData",
                ],
                "Resource": "*",
                "Condition": {
                    "StringEquals": {
                        "cloudwatch:namespace": CW_NAMESPACE,
                    }
                },
            },
            {
                "Sid": "S3ReadWriteValidation",
                "Effect": "Allow",
                "Action": [
                    "s3:GetObject",
                    "s3:PutObject",
                ],
                "Resource": [
                    "arn:aws:s3:::aip-c01-healthcare-demo-*/*",
                ],
            },
            {
                "Sid": "CloudWatchLogs",
                "Effect": "Allow",
                "Action": [
                    "logs:CreateLogGroup",
                    "logs:CreateLogStream",
                    "logs:PutLogEvents",
                ],
                "Resource": "arn:aws:logs:*:*:log-group:/aws/lambda/healthcare-clinical-validator:*",
            },
        ],
    },
}


def main():
    """Run validation locally against synthetic data with full Responsible AI checks."""
    records_path = DATA_DIR / "patient_records.json"
    notes_path = DATA_DIR / "clinical_notes.json"

    if not records_path.exists():
        print(f"ERROR: {records_path} not found. Run generate_healthcare_data.py first.")
        return

    with open(records_path) as f:
        records = json.load(f)

    # Load clinical notes and build record_id -> note_text map
    notes_map: dict[str, str] = {}
    if notes_path.exists():
        with open(notes_path) as f:
            notes_list = json.load(f)
        for note in notes_list:
            rid = str(note.get("record_id", ""))
            notes_map[rid] = note.get("note_text", "")
        print(f"Loaded {len(notes_map)} clinical notes from {notes_path.name}")
    else:
        print(f"WARNING: {notes_path} not found — Responsible AI checks will have no note text")

    print(f"Validating {len(records)} patient records with clinical domain rules...")
    print(f"Validation level: full (includes Responsible AI pre-FM gates)\n")

    # Simulate Lambda invocation
    event = {
        "records": records,
        "validation_level": "full",
        "emit_cloudwatch": False,  # Set True when AWS credentials available
        "clinical_notes": notes_map,
    }
    result = lambda_handler(event)

    summary = result["validation_summary"]
    ra_summary = result["responsible_ai_summary"]

    # ── Clinical Validation Report ──
    print(f"{'='*60}")
    print(f"CLINICAL VALIDATION REPORT")
    print(f"{'='*60}")
    print(f"Total records:    {summary['total']}")
    print(f"Passed:           {summary['passed']}")
    print(f"Warnings:         {summary['warnings']}")
    print(f"Failed:           {summary['failed']}")
    print(f"Quality score:    {summary.get('quality_score', 'N/A')}%")
    print(f"\nErrors by type:")
    for etype, count in sorted(summary["errors_by_type"].items(), key=lambda x: -x[1]):
        marker = " ← Responsible AI" if etype.startswith("RA_") else ""
        print(f"  {etype}: {count}{marker}")

    # ── Responsible AI Summary ──
    print(f"\n{'='*60}")
    print(f"RESPONSIBLE AI PRE-FM GATE SUMMARY")
    print(f"{'='*60}")
    print(f"Records checked:          {ra_summary['records_checked']}")
    print(f"Total RA issues found:    {ra_summary['total_ra_issues']}")
    print(f"  Profanity (ERROR):      {ra_summary['profanity_detected']}")
    print(f"  PHI exposure (WARNING): {ra_summary['phi_exposure_risk']}")
    print(f"  Low relevance:          {ra_summary['low_relevance']}")
    print(f"  Bias indicators:        {ra_summary['bias_indicators']}")
    print(f"  Low coherence:          {ra_summary['low_coherence']}")
    print(f"  Garbled text:           {ra_summary['garbled_text']}")
    print(f"  Unstructured notes:     {ra_summary['unstructured_notes']}")
    print(f"  Empty notes:            {ra_summary['empty_notes']}")
    print(f"  Avg clinical relevance: {ra_summary['avg_clinical_relevance_score']}%")

    blocked = ra_summary["profanity_detected"]
    if blocked > 0:
        print(f"\n  ⚠ {blocked} record(s) BLOCKED from FM consumption due to profanity")
    if ra_summary["phi_exposure_risk"] > 0:
        print(f"  ⚠ {ra_summary['phi_exposure_risk']} record(s) flagged for PHI — "
              f"route through Comprehend Medical de-identification")

    # ── Publish / save CloudWatch metrics ──
    ra_stats_for_metrics = {
        "profanity_detected": ra_summary["profanity_detected"],
        "phi_exposure_risk": ra_summary["phi_exposure_risk"],
        "avg_clinical_relevance_score": ra_summary["avg_clinical_relevance_score"],
    }
    all_metrics = build_metric_data(summary, ra_stats_for_metrics)

    # Attempt CloudWatch publish with graceful fallback
    publish_metrics(all_metrics, use_cloudwatch=False)

    # Save results
    with open(OUTPUT_DIR / "validation_results.json", "w") as f:
        json.dump(summary, f, indent=2)

    with open(OUTPUT_DIR / "flagged_records.json", "w") as f:
        json.dump(result["flagged_records"], f, indent=2, default=str)

    with open(OUTPUT_DIR / "responsible_ai_summary.json", "w") as f:
        json.dump(ra_summary, f, indent=2)

    # Save only validated (clean) records for downstream
    clean_records = [r for r in result["validated_records"]
                     if r["_validation"]["status"] in ("PASS", "WARNING")]
    with open(OUTPUT_DIR / "validated_records.json", "w") as f:
        # Strip internal validation metadata for downstream consumption
        output = []
        for r in clean_records:
            record = {k: v for k, v in r.items() if k != "_validation"}
            output.append(record)
        json.dump(output, f, indent=2)

    # Save Lambda deployment config
    with open(OUTPUT_DIR / "lambda_config.json", "w") as f:
        json.dump(LAMBDA_DEPLOYMENT_CONFIG, f, indent=2)

    print(f"\n{'='*60}")
    print(f"FILES SAVED")
    print(f"{'='*60}")
    print(f"Output directory: {OUTPUT_DIR}")
    print(f"  - validation_results.json      (summary)")
    print(f"  - flagged_records.json          ({len(result['flagged_records'])} flagged)")
    print(f"  - validated_records.json        ({len(clean_records)} clean records)")
    print(f"  - responsible_ai_summary.json   (RA gate results)")
    print(f"  - cloudwatch_metrics.json       ({len(all_metrics)} metrics)")
    print(f"  - lambda_config.json            (deployment configuration)")


if __name__ == "__main__":
    main()
