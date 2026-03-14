# Demo 1: Healthcare Records Processing Pipeline

## Real-World Context

A hospital network needs to process thousands of patient discharge summaries, lab reports, and
clinical notes before feeding them to a Bedrock foundation model for clinical decision support.
The data arrives from legacy EHR systems and contains abbreviations, missing fields, invalid
ICD-10 codes, and encoding inconsistencies. Before the FM can generate accurate clinical
summaries, every record must be validated, standardized, and formatted.

This demo mirrors the real ingestion problem faced by healthcare AI teams: structured tabular
data (patient demographics, vitals, diagnoses) and unstructured free-text (clinical notes)
arrive together but require very different validation strategies. AWS Glue Data Quality handles
schema-level and statistical rules on the CSV; a custom Lambda function enforces clinical domain
logic; and Amazon Comprehend Medical extracts entities from the notes to enrich the final prompt.

The resulting pipeline produces audit-ready CloudWatch metrics for compliance reporting and
Bedrock-ready JSON payloads that conform exactly to the Claude 3 Messages API, enabling the FM
to receive full patient context — structured data plus NLP-enriched note entities — in a single
well-formed request.

## Requirements Covered

| Section | Bullet | Requirement |
|---------|--------|-------------|
| 1.1 | ✅ | AWS Glue Data Quality rules for completeness, consistency, accuracy, and format |
| 1.3 | ✅ | Custom Lambda function (`lambda_handler`) for domain-specific clinical validation |
| 1.4 | ✅ | CloudWatch metrics, alarms, and dashboard configuration for quality monitoring |
| 1.5 | ✅ | Validation of both structured records (CSV) and unstructured clinical notes (JSON) |
| 3.1 | ✅ | Properly formatted JSON payloads for Bedrock `invoke_model` API |
| 4.2 | ✅ | Comprehend Medical entity extraction and standardization from clinical text |

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│  STEP 1 — Synthetic Data Generation                                 │
│  synth_data/generate_healthcare_data.py                             │
│                                                                     │
│  Outputs:                                                           │
│    synth_data/output/patient_records.csv   ← structured (Glue DQ)   │
│    synth_data/output/patient_records.json  ← structured (Lambda)    │
│    synth_data/output/clinical_notes.json   ← unstructured (Bedrock) │
└──────────────┬──────────────────────────┬───────────────────────────┘
               │                          │
               ▼                          ▼
┌──────────────────────────┐   ┌──────────────────────────────────────┐
│  STEP 2 — Glue DQ        │   │  STEP 3 — Lambda Domain Validation   │
│  glue_quality/           │   │  lambda_validation/                  │
│  glue_dq_rules.py        │   │  clinical_validator.py               │
│                          │   │                                      │
│  DQDL rules: completeness│   │  ICD-10 validation, vital sign       │
│  uniqueness, consistency,│   │  plausibility, drug interaction      │
│  range, format, stats    │   │  checks, date logic, LOS mismatch    │
│                          │   │                                      │
│  Outputs:                │   │  Outputs:                            │
│  glue_quality/output/    │   │  lambda_validation/output/           │
│    glue_dq_report.json   │   │    validated_records.json ──────────┐│
│    glue_dq_ruleset.dqdl  │   │    validation_results.json          ││
│    glue_dq_api_example   │   │    flagged_records.json             ││
└──────────┬───────────────┘   │    lambda_config.json               ││
           │                   └──────────────┬───────────────────────┘│
           │                                  │                        │
           └──────────┬───────────────────────┘                        │
                      │ (both feed CloudWatch)                         │
                      ▼                                                │
        ┌─────────────────────────────┐                                │
        │  STEP 4 — CloudWatch        │                                │
        │  cloudwatch/                │                                │
        │  quality_dashboard.py       │                                │
        │                             │                                │
        │  Generates:                 │                                │
        │  • Metric data (put_metric) │                                │
        │  • Alarm definitions        │                                │
        │  • Dashboard JSON body      │                                │
        │                             │                                │
        │  cloudwatch/output/         │                                │
        │    cloudwatch_metrics.json  │                                │
        │    cloudwatch_alarms.json   │                                │
        │    cloudwatch_dashboard.json│                                │
        └─────────────────────────────┘                                │
                                                                       │
        ┌──────────────────────────────────────────────────────────────┘
        │ validated_records.json + clinical_notes.json
        ▼
┌────────────────────────────────────────────────────────────────────┐
│  STEP 5 — Comprehend Entity Extraction + Bedrock Formatting        │
│  bedrock_formatting/format_for_bedrock.py                          │
│                                                                    │
│  • Simulates Comprehend Medical DetectEntitiesV2                   │
│    (MEDICATION, MEDICAL_CONDITION, PHI categories)                 │
│  • Standardizes and normalizes extracted entities                  │
│  • Builds Claude 3 Messages API payloads via bedrock_helpers.py    │
│                                                                    │
│  bedrock_formatting/output/                                        │
│    bedrock_payloads.json          ← 20 Bedrock-ready payloads      │
│    bedrock_payload_example.json   ← single annotated example       │
│    entity_extraction_results.json ← NLP entity data per record     │
│    comprehend_api_reference.json  ← real AWS API call templates    │
└────────────────────────────────────────────────────────────────────┘
```

## Files

| File | Purpose |
|------|---------|
| `synth_data/generate_healthcare_data.py` | Generate 200 synthetic patient records (CSV + JSON) and 200 clinical notes (JSON) with ~12% intentional quality issues |
| `glue_quality/glue_dq_rules.py` | DQDL ruleset definition and local simulation of AWS Glue Data Quality (completeness, uniqueness, consistency, range, format, statistical) |
| `lambda_validation/clinical_validator.py` | `lambda_handler`-compatible function for clinical domain validation: ICD-10 codes, vital sign plausibility, drug interactions, date logic |
| `cloudwatch/quality_dashboard.py` | Generates CloudWatch `put_metric_data` payloads, alarm configurations, and a full dashboard JSON body from upstream validation results |
| `bedrock_formatting/format_for_bedrock.py` | Simulates Comprehend Medical entity extraction from clinical notes; constructs Bedrock Claude 3 Messages API payloads |
| `../shared/utils/bedrock_helpers.py` | Shared utility: `build_bedrock_messages_payload()` — produces correctly structured `invoke_model` request dicts |

> **Note on local execution:** All scripts run offline with standard-library Python only — no
> third-party packages and no AWS credentials required. AWS API calls (`boto3`) are represented
> as generated configuration files and inline comments. Comprehend Medical is simulated with a
> keyword-matching engine that mirrors the real `DetectEntitiesV2` response schema.

## Step-by-Step Walkthrough

Run all steps from inside the `demo-1-healthcare-records/` directory:

```bash
cd aip-c01-1.3-demos/demo-1-healthcare-records
```

### Step 1: Generate Synthetic Healthcare Data

```bash
python synth_data/generate_healthcare_data.py
```

Produces three output files in `synth_data/output/`:

| File | Records | Format | Used by |
|------|---------|--------|---------|
| `patient_records.csv` | 200 | Flattened CSV | Glue DQ (Step 2) |
| `patient_records.json` | 200 | Nested JSON | Lambda validation (Step 3) |
| `clinical_notes.json` | 200 | Free-text notes | Bedrock formatting (Step 5) |

Approximately 12% of records contain intentional quality issues: missing names, future discharge
dates, negative ages, empty medication lists, invalid ICD-10 codes, and out-of-range vitals.

### Step 2: Run Glue Data Quality Checks

```bash
python glue_quality/glue_dq_rules.py
```

Applies the DQDL ruleset (defined in `GLUE_DQ_RULESET`) to `patient_records.csv`. Outputs to
`glue_quality/output/`:

- `glue_dq_report.json` — per-rule pass/fail results with violation details
- `glue_dq_ruleset.dqdl` — the raw DQDL string ready for `glue.create_data_quality_ruleset()`
- `glue_dq_api_example.json` — full `CreateDataQualityRuleset` and `StartDataQualityRuleRecommendationRun` API call structures

### Step 3: Apply Lambda Domain Validation

```bash
python lambda_validation/clinical_validator.py
```

Invokes `lambda_handler({"records": [...], "validation_level": "full"})` against the 200 JSON
records. Validation checks include: required field presence, age range, ICD-10 format, vital
sign plausibility with critical-threshold detection, drug interaction cross-referencing
(Warfarin+Aspirin, Lisinopril+Furosemide, Metformin+Furosemide), and date consistency.

Outputs to `lambda_validation/output/`:

- `validation_results.json` — summary counts (passed/warned/failed) and per-error-type breakdown
- `flagged_records.json` — all records that failed or triggered warnings
- `validated_records.json` — clean records (PASS + WARNING status) stripped of `_validation` metadata, ready for downstream processing
- `lambda_config.json` — `create_function` deployment configuration for production Lambda

### Step 4: Generate CloudWatch Monitoring Configurations

```bash
python cloudwatch/quality_dashboard.py
```

Reads `glue_quality/output/glue_dq_report.json` and `lambda_validation/output/validation_results.json`
(falls back to sample data if those files are not present). Generates production-ready AWS
CloudWatch configurations in `cloudwatch/output/`:

- `cloudwatch_metrics.json` — `put_metric_data` payload for namespace `AIP-C01/HealthcareDataQuality`, including per-error-type dimensions
- `cloudwatch_alarms.json` — three alarm definitions: DQ score < 80%, error count > 20, drug interaction count > 5
- `cloudwatch_dashboard.json` — full `put_dashboard` body with 5 widgets (time-series, bar charts, single-value KPI)
- `publish_metrics_example.json` — annotated boto3 API call example with batching logic

### Step 5: Extract Entities and Format for Bedrock

```bash
python bedrock_formatting/format_for_bedrock.py
```

Reads `lambda_validation/output/validated_records.json` (falls back to raw records if absent)
and `synth_data/output/clinical_notes.json`. For each of the first 20 records:

1. Passes the clinical note text through `simulate_comprehend_medical()`, which returns entities
   in the exact `DetectEntitiesV2` response schema (MEDICATION, MEDICAL_CONDITION,
   PROTECTED_HEALTH_INFORMATION categories with offset, confidence score, and traits).
2. Standardizes and normalizes entity results (e.g., maps "diabetes" → "Type 2 diabetes mellitus").
3. Merges structured record fields + NLP entity context into a "Patient Record Analysis Request"
   prompt and calls `build_bedrock_messages_payload()` with `temperature=0.1`, `top_p=0.9`,
   `max_tokens=2048`.

Outputs to `bedrock_formatting/output/`:

- `entity_extraction_results.json` — per-record entity data (medications, conditions, PHI flag)
- `bedrock_payloads.json` — 20 complete Bedrock request payloads
- `bedrock_payload_example.json` — single pretty-printed example
- `comprehend_api_reference.json` — `detect_entities_v2`, `detect_phi`, and `infer_icd10_cm` API call templates

## Expected Output Format

The `body` field in a Bedrock `invoke_model` request is a **JSON-serialized string** (not a
nested object). The outer structure and the parsed body are shown separately below.

**Outer `invoke_model` payload** (as returned by `build_bedrock_messages_payload()`):

```json
{
    "modelId": "anthropic.claude-3-sonnet-20240229-v1:0",
    "contentType": "application/json",
    "accept": "application/json",
    "body": "<JSON-serialized string — see parsed content below>"
}
```

**Parsed `body` content** (what you get after `json.loads(payload["body"])`):

```json
{
    "anthropic_version": "bedrock-2023-05-31",
    "max_tokens": 2048,
    "temperature": 0.1,
    "top_p": 0.9,
    "system": "You are a clinical decision support assistant for healthcare professionals.\nYour role is to analyze patient records and provide evidence-based clinical summaries.\n\nGuidelines:\n- Use standard medical terminology\n- Reference ICD-10 codes when applicable\n- Flag potential drug interactions or contraindications\n...",
    "messages": [
        {
            "role": "user",
            "content": "Patient Record Analysis Request\n================================\nPatient: Mary Johnson, Age: 67, Gender: F\nMRN: MRN-000042\nDepartment: Cardiology\nAttending: Dr. Williams\n\nAdmission: 2026-02-15 → Discharge: 2026-02-22\nLength of Stay: 7 days\n\nDiagnoses:\n  - Heart failure, unspecified (ICD-10: I50.9)\n  - Essential (primary) hypertension (ICD-10: I10)\n\nMedications:\n  - Furosemide 40mg BID\n  - Lisinopril 10mg QD\n  - Atorvastatin 40mg QHS\n\nVitals at Discharge: systolic_bp: 138, diastolic_bp: 82, heart_rate: 76, temperature_f: 98.4\n\nNLP-Extracted Conditions: Heart failure (confidence: 92%), hypertension (confidence: 92%)\nNLP-Extracted Medications: Furosemide, Lisinopril, Atorvastatin\n\nClinical Note:\nDischarge summary — Mary Johnson admitted on 2026-02-15 for Heart failure...\n\nPlease provide a comprehensive clinical summary with medication review and follow-up recommendations."
        }
    ]
}
```

> **Key Bedrock API detail:** When calling `bedrock-runtime.invoke_model()`, pass `body` as a
> string: `body=json.dumps(parsed_body)` or use the dict returned directly by
> `build_bedrock_messages_payload()` which already serializes `body` via `json.dumps()`.

## Success Criteria

1. ✅ Synthetic data includes ≥10% records with intentional quality issues (missing fields, invalid codes, out-of-range vitals, future dates, negative ages)
2. ✅ Glue DQ DQDL ruleset covers all five rule types: Completeness, Uniqueness, Consistency (CustomSql), Range (ColumnValues), and Format (pattern matching)
3. ✅ Lambda `lambda_handler` catches domain-specific clinical issues: invalid ICD-10 codes, vital sign range and critical-threshold violations, drug interaction flags (HIGH/MODERATE/LOW severity), date logic errors, and LOS mismatches
4. ✅ CloudWatch configuration defines metrics in namespace `AIP-C01/HealthcareDataQuality` with multi-dimensional tagging, at least 3 alarms, and a 5-widget dashboard
5. ✅ Comprehend entity extraction (simulated locally with real `DetectEntitiesV2` response schema) identifies MEDICATION, MEDICAL_CONDITION, and PHI entities from clinical note text; in production, replace `simulate_comprehend_medical()` with `boto3.client('comprehendmedical').detect_entities_v2()`
6. ✅ Final Bedrock payloads conform to Claude 3 Sonnet Messages API format with `anthropic_version`, `max_tokens: 2048`, `temperature: 0.1`, and a multi-section system prompt defining the clinical decision support role
