# AWS Glue Data Quality — End-to-End Explained

> **Scope:** This document traces the complete Glue Data Quality pipeline for
> Demo 1: Healthcare Records — from raw synthetic data on disk all the way to
> a quality report in the AWS console. Every resource name, CLI command, and
> code snippet is pulled directly from the actual source files.

---

## Table of Contents

1. [Folder Structure & Why It Exists](#1-folder-structure--why-it-exists)
2. [Who Creates DQ Rulesets?](#2-who-creates-dq-rulesets)
3. [The Chain of Events — Step by Step](#3-the-chain-of-events--step-by-step)
   - [Steps 1–2: S3 Bucket + Data Upload](#steps-12-s3-bucket--data-upload)
   - [Steps 3–4: IAM Role + Glue Database](#steps-34-iam-role--glue-database)
   - [Step 4b: Lake Formation Permissions — The Hidden Requirement](#step-4b-lake-formation-permissions--the-hidden-requirement)
   - [Step 5: The Glue Crawler — The Key Step](#step-5-the-glue-crawler--the-key-step)
   - [Step 6: The Glue DQ Ruleset](#step-6-the-glue-dq-ruleset)
   - [Steps 6b–6c: Evaluation Runs & Rule Recommendations](#steps-6b6c-evaluation-runs--rule-recommendations)
4. [The Dependency Chain Diagram](#4-the-dependency-chain-diagram)
5. [DQDL Rule Reference](#5-dqdl-rule-reference)
6. [Anomaly Detection & Analyzers](#6-anomaly-detection--analyzers)
7. [Local vs. AWS Execution](#7-local-vs-aws-execution)
8. [Reading the Quality Report](#8-reading-the-quality-report)
9. [Production Usage Patterns](#9-production-usage-patterns)

---

## 1. Folder Structure & Why It Exists

```
demo-1-healthcare-records/
│
├── synth_data/
│   ├── generate_healthcare_data.py   ← Produces the 3 data files below
│   └── output/
│       ├── patient_records.csv       ← Flat table — Glue DQ primary input
│       ├── patient_records.json      ← Structured records (nested JSON)
│       └── clinical_notes.json       ← Unstructured clinical text
│
├── glue_quality/                     ← Distinct AWS Glue service layer
│   ├── glue_dq_rules.py              ← DQDL definitions + local simulator (all 3 tables)
│   └── output/
│       ├── glue_dq_ruleset_patient_records_csv.dqdl   ← 19 rules + 10 analyzers (flat CSV)
│       ├── glue_dq_ruleset_patient_records_json.dqdl  ← 23 rules + 8 analyzers (nested JSON)
│       ├── glue_dq_ruleset_clinical_notes_json.dqdl   ← 14 rules + 7 analyzers (text/notes)
│       ├── glue_dq_ruleset.dqdl      ← Original single-table ruleset (legacy reference)
│       ├── glue_dq_report.json       ← Local simulation results
│       └── glue_dq_api_example.json  ← boto3 API call reference
│
├── scripts/
│   └── setup-aws-infra.sh            ← Provisions all AWS resources (Steps 1–9)
│
└── ...
```

### Why `glue_quality/` is its own folder

`glue_quality/` is **not** just another Python script — it represents a
distinct AWS service layer. AWS Glue Data Quality operates on tables registered
in the **Glue Data Catalog**, not directly on S3 files. The folder boundary
reflects that architectural boundary:

| Layer | Where it lives | What it touches |
|-------|---------------|-----------------|
| Raw data generation | `synth_data/` | Local filesystem |
| Schema registration | AWS Glue Crawler (Step 5) | Glue Data Catalog |
| **Quality rules** | `glue_quality/` | Glue Data Catalog table |
| Quality evaluation | AWS Glue DQ evaluation run | Reads S3 via catalog |

### `glue_dq_rules.py` has two jobs

```
glue_dq_rules.py
  │
  ├── Job 1 — LOCAL SIMULATION (all 3 tables)
  │     Reads patient_records.csv, patient_records.json, clinical_notes.json directly
  │     Applies the same logic as DQDL rules in pure Python (56 rules total)
  │     Writes glue_dq_report.json   ← combined pass/fail results locally
  │
  └── Job 2 — DQDL EXPORT (one file per table)
        Writes 3 DQDL files to glue_quality/output/:
          glue_dq_ruleset_patient_records_csv.dqdl   (19 rules + 10 analyzers)
          glue_dq_ruleset_patient_records_json.dqdl  (23 rules + 8 analyzers)
          glue_dq_ruleset_clinical_notes_json.dqdl   (14 rules + 7 analyzers)
        setup-aws-infra.sh loops over all 3 and uploads each to AWS
```

This dual-purpose design lets you **test rules without AWS credentials** while
guaranteeing the same rule text runs in the cloud.

---

## 2. Who Creates DQ Rulesets?

In most organizations, DQ rulesets are **not** authored by data scientists.
Understanding who owns DQDL authorship — and why — is important context both
for real-world pipeline design and for the AIP-C01 exam, which emphasizes
organizational roles in AI/ML data preparation.

### The responsibility breakdown

| Persona | Role in the DQ Pipeline | Why |
|---------|------------------------|-----|
| **Data Engineers** | **Primary authors** of DQDL rulesets | They own ETL pipelines, understand table schemas end-to-end, and are directly accountable for data reliability SLAs. They translate business requirements into concrete DQDL syntax. |
| **Domain / Business SMEs** | Define the business rules in plain language — e.g., *"patient age must be 0–120"*, *"ICD-10 codes must follow the standard format"*, *"a patient cannot be discharged before they were admitted"* | They know what *correct* looks like for the domain, but they do not write DQDL themselves. Their rules become requirements handed off to data engineers. |
| **Data Scientists** | Consumers who benefit from clean data. They surface quality problems discovered during EDA — e.g., *"I'm seeing extreme outliers in `systolic_bp`"* — which feed back to data engineers as rule requests. Rarely write DQDL directly. | They work at the feature/model layer, not the raw-data governance layer. Their feedback loop is essential, but they are rule *consumers*, not rule *authors*. |
| **ML / GenAI Engineers** | In the Bedrock / FM context, they specify the quality requirements the model needs — e.g., *"no nulls in any field used in the prompt"*, *"clinical notes must be under 4096 tokens"*, *"ICD-10 codes must be valid for code-to-text mapping"*. These requirements flow to data engineers as formal DQ rule requests. | They understand model inputs and failure modes but operate above the data pipeline layer. Their quality specifications become the upstream input to DQ rule authorship. |

### The typical authorship workflow

```
Business SME                 Data Engineer              ML/GenAI Engineer
─────────────                ─────────────              ─────────────────
"age must be 0–120"   ──►   Writes DQDL rules    ◄──  "no nulls in prompt
"ICD-10 must be             (primary author)            fields; text < 4096
 valid"                     runs auto-recommend          tokens"
"no retroactive             first, then customizes
 dates"                            │
                                   ▼
                        glue_quality/output/
                        glue_dq_ruleset.dqdl
                                   │
                                   ▼
                        aws glue create-data-quality-ruleset
                        --target-table
                          demo_1_healthcare_records_pdx.raw_data
                                   │
                                   ▼
                        Ruleset attached to Glue Data Catalog table
                        (ready for evaluation runs)
```

### The auto-recommend shortcut

AWS Glue Data Quality includes a rule **auto-recommendation** feature that
analyzes the actual data and suggests an initial set of DQDL rules — so the
data engineer does not start from scratch. This is the standard production
onboarding workflow:

```
1. Run:   aws glue start-data-quality-rule-recommendation-run
              --data-source '{"GlueTable": {"DatabaseName": "...", "TableName": "..."}}'
              --role <glue-role-arn>

2. Poll:  aws glue get-data-quality-rule-recommendation-run --run-id <id>
              → wait until  Status = "SUCCEEDED"

3. Fetch: aws glue get-data-quality-rule-recommendation-run --run-id <id>
              → RecommendedRuleset field contains auto-generated DQDL

4. Review: engineer examines each suggested rule —
           accepts, adjusts thresholds, or removes irrelevant rules

5. Formalize: customized rules saved to glue_dq_ruleset.dqdl
              → uploaded via create-data-quality-ruleset

6. Attach: --target-table binds the ruleset to the catalog table
```

Auto-recommend is particularly valuable for wide tables where writing 17+
completeness and range rules by hand would be tedious and error-prone. The
suggested rules reflect actual data distributions, giving sensible default
thresholds that the engineer can then tighten to meet business requirements.

> **In this demo:** `glue_dq_rules.py` defines all 17 rules explicitly — written
> by the data engineer — which makes the threshold choices and rule categories
> fully transparent for learning purposes. In a production onboarding, you would
> run auto-recommend first, then customize the output.

---

## 3. The Chain of Events — Step by Step

`setup-aws-infra.sh` runs nine steps. Steps 1–6 build the Glue DQ pipeline.

### Steps 1–2: S3 Bucket + Data Upload

**Step 1** creates the S3 bucket:

```bash
aws s3api create-bucket \
    --bucket demo-1-healthcare-records-pdx \
    --region us-west-2 \
    --create-bucket-configuration "LocationConstraint=us-west-2"
```

The bucket is private (public access blocked) and tagged with
`Project=aip-c01-demos, Demo=healthcare-records`.

**Step 2** uploads all three data files into the `/raw-data/` prefix, with an important
conversion step for the JSON files:

```bash
# CSV — uploaded as-is (flat format works directly with Glue JSON SerDe)
aws s3 cp synth_data/output/patient_records.csv \
    s3://demo-1-healthcare-records-pdx/raw-data/patient_records.csv

# JSON files — MUST be converted to JSON Lines before upload
# (see AWS_AI_LEARNINGS.md entry #14 — JSON arrays create single-column tables)
python3 -c "
import json, sys
with open(sys.argv[1]) as f:
    for item in json.load(f):
        print(json.dumps(item, ensure_ascii=False))
" synth_data/output/patient_records.json > /tmp/patient_records.json
aws s3 cp /tmp/patient_records.json s3://demo-1-healthcare-records-pdx/raw-data/patient_records.json

python3 -c "
import json, sys
with open(sys.argv[1]) as f:
    for item in json.load(f):
        print(json.dumps(item, ensure_ascii=False))
" synth_data/output/clinical_notes.json > /tmp/clinical_notes.json
aws s3 cp /tmp/clinical_notes.json s3://demo-1-healthcare-records-pdx/raw-data/clinical_notes.json
```

> **⚠️ JSON Lines conversion is mandatory — [AWS_AI_LEARNINGS.md entry #14]**
> The raw `patient_records.json` and `clinical_notes.json` files contain top-level JSON
> arrays: `[{...}, {...}, ...]`. When the Glue crawler encounters a JSON array, the OpenX
> SerDe treats the **entire array as one row**, creating a table with a single column named
> `array` of type `array<struct<...>>`. This makes DQDL rules useless — there are no
> individual columns to validate. Converting to JSON Lines (one object per line, no outer
> array) causes the crawler to create proper multi-row tables with individual columns.
>
> | Format | S3 File Content | Crawler Result |
> |--------|----------------|----------------|
> | JSON Array | `[{"a":1}, {"a":2}]` | 1 row, 1 column `array` |
> | JSON Lines | `{"a":1}\n{"a":2}\n` | 2 rows, 1 column `a` |

After Step 2, the bucket looks like this:

```
s3://demo-1-healthcare-records-pdx/
└── raw-data/
    ├── patient_records.csv       ← 200 rows, 17 columns (flat CSV — unchanged)
    ├── patient_records.json      ← 200 JSON Lines (converted from array)
    └── clinical_notes.json       ← 200 JSON Lines (converted from array)
```

> **Important:** The data is in S3 but it is **invisible to AWS analytics
> services** (Athena, Glue ETL, Glue DQ) until it is registered in the
> Glue Data Catalog. That registration happens in Step 5.

---

### Steps 3–4: IAM Role + Glue Database

**Step 3** creates the IAM role that Glue will assume:

```
Role name:  AWSGlueServiceRole-HealthRecords
Trust:      glue.amazonaws.com
Policies:
  • AWSGlueServiceRole (managed) — lets Glue call other AWS services
  • S3ReadAccess-demo-1-healthcare-records-pdx (inline):
      s3:GetObject  on arn:aws:s3:::demo-1-healthcare-records-pdx/*
      s3:ListBucket on arn:aws:s3:::demo-1-healthcare-records-pdx
```

**Step 4** creates the Glue database:

```bash
aws glue create-database \
    --database-input '{
        "Name": "demo_1_healthcare_records_pdx",
        "Description": "Healthcare patient records for Demo 1 — AIP-C01"
    }'
```

A Glue database is analogous to a schema in a traditional RDBMS — it is a
**namespace only**. No data lives in it. It holds zero tables at this point.
Think of it as an empty filing cabinet: you have the cabinet (database) but no
folders inside it yet. The crawler in Step 5 creates the folders (tables).

```
Before Step 5:
  Glue Catalog
  └── demo_1_healthcare_records_pdx   (empty database — no tables)

After Step 5:
  Glue Catalog
  └── demo_1_healthcare_records_pdx
      ├── patient_records_csv         (auto-named from patient_records.csv)
      ├── patient_records_json        (auto-named from patient_records.json — JSON Lines)
      └── clinical_notes_json         (auto-named from clinical_notes.json — JSON Lines)
```

> **⚠️ Table names are auto-generated, not user-defined.** `GLUE_TABLE_NAME="raw_data"` in
> `setup-aws-infra.sh` is a *fallback default only*. Actual table names are derived from the
> S3 file names — `patient_records.csv` → `patient_records_csv`, etc. Step 5 dynamically
> discovers all table names via `get-tables` after the crawler completes. See
> [AWS_AI_LEARNINGS.md entry #10] for the full story.

---

### Step 4b: Lake Formation Permissions — The Hidden Requirement

> **This step does not exist in many tutorials — which is exactly why crawlers silently fail
> in modern AWS accounts.** It was added to `setup-aws-infra.sh` after a production failure
> described in [AWS_AI_LEARNINGS.md entry #11].

Modern AWS accounts have Lake Formation enabled by default. Lake Formation maintains its own
permission model **on top of IAM** — two locks on the same door. Even if the Glue crawler
role has the full `AWSGlueServiceRole` managed policy and S3 read access (Step 3), it will
fail with `AccessDeniedException` when it tries to write table metadata to the catalog.

Worse, this failure is *silent from the outside*: the crawler's `LastCrawl.Status` reports
`SUCCEEDED` (because it crawled the files successfully), but the resulting tables are
**invisible** to any subsequent API call. `get-tables` returns `{"TableList": []}` as if no
tables exist. This causes Step 6 (`create-data-quality-ruleset`) to fail with the misleading
`EntityNotFoundException` — the table truly can't be found through the LF permission layer.

#### What Step 4b provisions

Two grants are made in `grant_lakeformation_permissions()`:

**Part A — DATABASE-level permissions (for the Glue crawler role):**

```bash
aws lakeformation grant-permissions \
    --principal "DataLakePrincipalIdentifier=${role_arn}" \
    --permissions "CREATE_TABLE" "DESCRIBE" "ALTER" \
    --resource '{"Database": {"Name": "'"${GLUE_DB_NAME}"'"}}' \
    --region "${AWS_REGION}"
```

This allows the crawler to create and update table entries in the database. Without this,
the crawler cannot write to the catalog at all.

**Part B — TABLE wildcard permissions (for both the Glue role AND the caller/deployer):**

```bash
# For the Glue crawler role — so it can create and read the tables it creates
aws lakeformation grant-permissions \
    --principal "DataLakePrincipalIdentifier=${glue_role_arn}" \
    --permissions "ALL" "SELECT" "ALTER" "DROP" "DELETE" "INSERT" "DESCRIBE" \
    --permissions-with-grant-option "ALL" "SELECT" "ALTER" "DROP" "DELETE" "INSERT" "DESCRIBE" \
    --resource '{"Table": {"DatabaseName": "'"${GLUE_DB_NAME}"'", "TableWildcard": {}}}' \
    --region "${AWS_REGION}"

# For the deployer/caller role — so get-tables works during script execution
aws lakeformation grant-permissions \
    --principal "DataLakePrincipalIdentifier=${caller_role_arn}" \
    --permissions "ALL" "SELECT" "ALTER" "DROP" "DELETE" "INSERT" "DESCRIBE" \
    --permissions-with-grant-option "ALL" "SELECT" "ALTER" "DROP" "DELETE" "INSERT" "DESCRIBE" \
    --resource '{"Table": {"DatabaseName": "'"${GLUE_DB_NAME}"'", "TableWildcard": {}}}' \
    --region "${AWS_REGION}"
```

The `TableWildcard: {}` covers all current **and future** tables in the database. This is
what makes the crawler-created tables visible to subsequent `get-tables` calls.

#### Why DATABASE permissions alone are not enough

```
DATABASE permissions   →  "can you create/modify the database and its metadata?"
TABLE permissions      →  "can you see/read/modify the tables inside the database?"
```

Without TABLE wildcard permissions, the tables physically exist in the catalog but are
invisible — Lake Formation silently filters them out of `get-tables` results. The only
way to detect their actual existence is to call `get-table` by the exact name, which
returns `AccessDeniedException` (not `EntityNotFoundException`), revealing that the table
exists but is blocked.

#### Idempotency guard

`lakeformation grant-permissions` is **not idempotent** — calling it twice for the same
principal/resource pair throws an error. The script checks `list-permissions` first and
only grants if no existing entry is found.

---

### Step 5: The Glue Crawler — The Key Step

The crawler is the bridge between S3 files and the Glue Data Catalog.

#### What the crawler is configured to do

```bash
aws glue create-crawler \
    --name  demo-1-healthcarerecords-pdx-crawler \
    --role  arn:aws:iam::ACCOUNT_ID:role/AWSGlueServiceRole-HealthRecords \
    --database-name  demo_1_healthcare_records_pdx \
    --targets '{"S3Targets": [{"Path": "s3://demo-1-healthcare-records-pdx/raw-data/"}]}'
```

Then immediately started and polled until completion (up to 300 s):

```bash
aws glue start-crawler --name demo-1-healthcarerecords-pdx-crawler
```

#### What the crawler actually does internally

```
Crawler run sequence:
  1. Assumes AWSGlueServiceRole-HealthRecords
  2. Lists s3://demo-1-healthcare-records-pdx/raw-data/
  3. Samples patient_records.csv  → infers column names + data types (17 cols)
  4. Samples patient_records.json → infers nested schema (JSON Lines — proper columns)
  5. Samples clinical_notes.json  → infers schema (JSON Lines — text + metadata fields)
  6. Groups files by format/schema similarity
  7. Creates 3 table entries in demo_1_healthcare_records_pdx:
       patient_records_csv, patient_records_json, clinical_notes_json
```

#### What the catalog table looks like after the crawler

The crawler **auto-names** the table from the S3 path and file name — it does **not** use
any user-supplied name. For `s3://demo-1-healthcare-records-pdx/raw-data/patient_records.csv`,
the crawler strips the bucket prefix and derives `patient_records_csv`. The variable
`GLUE_TABLE_NAME="raw_data"` in the script is a **fallback default only** — Step 5 always
dynamically discovers the actual table name after the crawler completes:

```bash
# From setup-aws-infra.sh Step 5 — post-crawler table discovery
discovered_tables="$(aws glue get-tables \
    --database-name "${GLUE_DB_NAME}" \
    --region "${AWS_REGION}" \
    --query "TableList[].Name" \
    --output text \
    --no-cli-pager 2>/dev/null || echo "")"

if [[ -n "${discovered_tables}" && "${discovered_tables}" != "None" ]]; then
    DISCOVERED_TABLE_NAME="$(echo "${discovered_tables}" | awk '{print $1}')"
    # → DISCOVERED_TABLE_NAME = "patient_records_csv"
fi
```

`DISCOVERED_TABLE_NAME` is then passed to Step 6. If `get-tables` returns empty (e.g., due
to unresolved Lake Formation permissions — see Step 4b), the script falls back to
`GLUE_TABLE_NAME="raw_data"` and Step 6 uses the fallback path of creating the ruleset
without `--target-table`.

For `patient_records.csv` specifically, the crawler produces a table with these inferred columns:

| Column | Inferred Type | Source |
|--------|--------------|--------|
| `record_id` | `bigint` | CSV integer column |
| `mrn` | `string` | e.g. `MRN-000042` |
| `patient_name` | `string` | e.g. `James Smith` |
| `age` | `bigint` | integer 25–89 |
| `gender` | `string` | `M` / `F` |
| `admission_date` | `string` | `YYYY-MM-DD` |
| `discharge_date` | `string` | `YYYY-MM-DD` |
| `length_of_stay_days` | `bigint` | 1–14 |
| `primary_icd10` | `string` | e.g. `E11.9` |
| `primary_diagnosis` | `string` | description text |
| `num_medications` | `bigint` | 1–5 |
| `systolic_bp` | `bigint` | 100–160 (with outliers) |
| `diastolic_bp` | `bigint` | 60–100 |
| `heart_rate` | `bigint` | 55–105 |
| `temperature_f` | `double` | 97.0–99.5 (with outliers) |
| `department` | `string` | e.g. `Cardiology` |
| `attending_physician` | `string` | e.g. `Dr. Martinez` |

The table entry in the catalog stores:
- **Location:** `s3://demo-1-healthcare-records-pdx/raw-data/`
- **Input format:** `org.apache.hadoop.mapred.TextInputFormat`
- **Serde:** `org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe`
- **Schema:** all 17 columns with types (above)
- **Row count estimate:** from crawler statistics

> **The data never moves.** The catalog table is a metadata pointer only.
> S3 stays the system of record. The catalog entry is what makes the data
> queryable by Athena, Glue ETL jobs, and Glue Data Quality — all three
> read through the catalog, not directly from S3.

---

### Step 6: The Glue DQ Rulesets (Multi-Table)

The ruleset is attached to the catalog table, not to S3 directly. This is
critical: Glue DQ doesn't know about `patient_records.csv` as a file — it
knows about `demo_1_healthcare_records_pdx.patient_records_csv` as a catalog table. The
ruleset is the policy defining what "good data" means for that table.

Since the crawler creates **three** tables (one per file format), there are now **three
separate DQDL rulesets** — one per table. Each ruleset is tailored to the schema and
data characteristics of that table:

| Table | Ruleset File | Rules | Analyzers | Notable Techniques |
|-------|-------------|-------|-----------|-------------------|
| `patient_records_csv` | `glue_dq_ruleset_patient_records_csv.dqdl` | 19 | 10 | Range checks, ICD-10 regex, statistical |
| `patient_records_json` | `glue_dq_ruleset_patient_records_json.dqdl` | 23 | 8 | CustomSql for nested struct/array access |
| `clinical_notes_json` | `glue_dq_ruleset_clinical_notes_json.dqdl` | 14 | 7 | Text length, format regex, author match |

#### Creating the rulesets

`setup-aws-infra.sh` loops over all three tables, reads each corresponding `.dqdl` file,
and creates one ruleset per table. The `create_glue_dq_rulesets()` function (Step 6)
also handles migration: the legacy single-table ruleset (`healthcare-patient-records-dq-ruleset`)
is deleted if found, before creating the three per-table rulesets.

```bash
# From setup-aws-infra.sh — create_glue_dq_rulesets() — the core loop

# Arrays declared at top of script:
DQ_TABLE_NAMES=("patient_records_csv" "patient_records_json" "clinical_notes_json")
DQ_RULESET_NAMES=("healthcare-patient-records-csv-dq"
                  "healthcare-patient-records-json-dq"
                  "healthcare-clinical-notes-json-dq")
DQ_RULESET_FILES=("glue_dq_ruleset_patient_records_csv.dqdl"
                  "glue_dq_ruleset_patient_records_json.dqdl"
                  "glue_dq_ruleset_clinical_notes_json.dqdl")

for i in "${!DQ_TABLE_NAMES[@]}"; do
    local table_name="${DQ_TABLE_NAMES[$i]}"
    local ruleset_name="${DQ_RULESET_NAMES[$i]}"
    local ruleset_file="${DEMO_DIR}/glue_quality/output/${DQ_RULESET_FILES[$i]}"

    local dqdl_content
    dqdl_content="$(cat "${ruleset_file}")"

    if ${table_exists}; then
        aws glue create-data-quality-ruleset \
            --name "${ruleset_name}" \
            --ruleset "${dqdl_content}" \
            --target-table "{
                \"TableName\": \"${table_name}\",
                \"DatabaseName\": \"${GLUE_DB_NAME}\"
            }" \
            --region "${AWS_REGION}" \
            --no-cli-pager
    fi
done
```

**Four things to understand about this loop:**

1. **`--ruleset` takes content, not a file path.** The AWS CLI `--ruleset`
   parameter accepts the DQDL text string directly. The `$(cat ...)` bash
   substitution reads each `.dqdl` file and inlines the full text at the call
   site. There is no `--ruleset-file` shorthand.

2. **Each `.dqdl` file is its own complete ruleset.** Each file contains a
   `Rules = [ ... ]` block (and `Analyzers = [ ... ]` block) tailored for that
   table's schema. When `cat` reads it and bash passes the string to `--ruleset`,
   the entire DQDL definition is uploaded verbatim into the Glue Data Catalog.

3. **`--target-table` is optional but preferred.** `TargetTable` is optional in
   `CreateDataQualityRuleset`. When a table is found in the catalog, the script
   binds the ruleset at creation time so it appears linked to the table in the
   Glue console. If the table is not yet present, the ruleset is created without
   binding and the target table is supplied at evaluation time via `--data-source`.

4. **Legacy migration.** The original single-table ruleset `healthcare-patient-records-dq-ruleset`
   is detected and deleted on first run before the three per-table rulesets are
   created. This prevents orphaned rulesets from appearing in the Glue console.

> **Exam tip:** On the AIP-C01 exam, know that Glue DQ rulesets are attached to
> **Glue Data Catalog tables**, not directly to S3 paths. The catalog table —
> created by the crawler — is the required intermediary that makes the S3 data
> queryable and evaluable by Glue DQ. When multiple tables exist (e.g., flat CSV,
> nested JSON, text records), each should have its own tailored ruleset.

#### Each ruleset is a definition, not a run

`create-data-quality-ruleset` **registers** the rules — it does not evaluate
any data. No Glue job is spun up. No data is read. Each ruleset sits in the
catalog as a named, versioned policy document bound to its target table.

The actual evaluations happen in Step 6b, which loops over all three tables
and starts an evaluation run for each.

---

### Steps 6b–6c: Evaluation Runs & Rule Recommendations

Steps 6b and 6c are implemented in `setup-aws-infra.sh` as `run_dq_evaluation()` and
`run_dq_recommendation()`. They run after the ruleset is created (Step 6) and represent the
two serverless, fully managed Glue DQ capabilities that go beyond simply registering rules.

---

#### Step 6b — The Evaluation Run: Running Rules Against All 3 Tables

`create-data-quality-ruleset` (Step 6) registers the policy — it does **not** read any data.
Step 6b triggers the actual evaluation: Glue spins up a serverless Spark cluster, reads each
catalog table through Lake Formation, applies every rule in that table's ruleset, and produces a
scored quality report. The `run_dq_evaluations()` function loops over all three tables.

**CLI command pattern (from `scripts/setup-aws-infra.sh` — `run_dq_evaluations()`):**

```bash
# Called for each of the 3 tables: patient_records_csv, patient_records_json, clinical_notes_json
aws glue start-data-quality-ruleset-evaluation-run \
    --data-source "{
        \"GlueTable\": {
            \"DatabaseName\": \"demo_1_healthcare_records_pdx\",
            \"TableName\": \"<table_name>\"          # loops: csv → json → notes
        }
    }" \
    --role "arn:aws:iam::${AWS_ACCOUNT_ID}:role/AWSGlueServiceRole-HealthRecords" \
    --ruleset-names "<per-table-ruleset-name>"       # healthcare-patient-records-csv-dq, etc.
    --number-of-workers 2 \
    --timeout 10 \
    --additional-run-options "{
        \"CloudWatchMetricsEnabled\": true,
        \"ResultsS3Prefix\": \"s3://demo-1-healthcare-records-pdx/dq-results/<table_name>/\"
    }" \
    --region us-west-2
```

Each table gets its own evaluation run with results partitioned by table name under the
`dq-results/` S3 prefix. The script polls each run until `SUCCEEDED` before starting the
next, then calls `display_dq_result()` to print the score and per-rule breakdown.

```bash
# Poll until Status = SUCCEEDED (check every 15s — cluster startup takes ~2 min)
aws glue get-data-quality-ruleset-evaluation-run \
    --run-id <RunId> \
    --region us-west-2 \
    --query "{Status: Status, ResultIds: ResultIds}"

# Fetch full per-rule results when SUCCEEDED
aws glue get-data-quality-result \
    --result-id <ResultId> \
    --region us-west-2
```

#### What the evaluation runs produce

**1. DQ Scores (all 3 tables)** — actual AWS evaluation results:

```
  ┌──────────────────────────────────────────────────────────────────────────┐
  │  📊 Table: patient_records_csv   Score: 79.0%  (15/19 rules passed)     │
  │  📊 Table: patient_records_json  Score: 87.0%  (20/23 rules passed)     │
  │  📊 Table: clinical_notes_json   Score: 100.0% (14/14 rules passed)     │
  └──────────────────────────────────────────────────────────────────────────┘
```

**`patient_records_csv` — 4 failures** (intentional synthetic data quality issues):

| Rule | Result | Failure Reason |
|------|--------|----------------|
| `ColumnValues "age" between 0 and 120` | ❌ FAIL | 3 records with `age = -5` (`negative_age` injection) |
| `ColumnValues "systolic_bp" between 50 and 250` | ❌ FAIL | 3 extreme outliers (300–500 range) **plus** NULL rows treated as failures |
| `ColumnValues "diastolic_bp" between 30 and 150` | ❌ FAIL | NULL `diastolic_bp` values treated as range failures |
| `ColumnValues "temperature_f" between 90.0 and 108.0` | ❌ FAIL | 3 records with 112.0–114.1°F (`extreme_temp` injection) |

**`patient_records_json` — 3 failures** (nested data + injected issues):

| Rule | Result | Failure Reason |
|------|--------|----------------|
| `ColumnValues "age" between 0 and 120` | ❌ FAIL | Same `age = -5` records as CSV |
| `ColumnValues "gender" in ["M", "F"]` | ❌ FAIL | 1 record with injected invalid gender code |
| `CustomSql "SELECT COUNT(*) FROM primary WHERE diagnoses IS NULL" <= 10` | ❌ FAIL | 12 NULL `diagnoses` values exceeded threshold |

**`clinical_notes_json` — 0 failures** — all 14 rules pass on the first real AWS run:
the injected issues (missing notes, invalid authors) fell within rule thresholds for this table.

> **⚠️ NULL-as-failure gotcha — [AWS_AI_LEARNINGS.md entry #13]**
> `ColumnValues` range rules treat NULL values as failing the range check. Rows where
> `systolic_bp IS NULL` fail `between 50 and 250` even though they're not technically
> out of range — they're simply absent. The separate `Completeness "systolic_bp" >= 0.90`
> rule _passes_ (0.5% nulls ≪ 10% threshold), but the range rule _fails_ those same rows
> again. Fix options: add `with threshold >= 0.95` to the ColumnValues rule, or use
> `CustomSql "SELECT COUNT(*) FROM primary WHERE col IS NOT NULL AND (col < 50 OR col > 250)" = 0`
> to exclude NULLs from the range check. See [Section 5](#5-dqdl-rule-reference) for
> annotated examples.

**2. Per-rule `EvaluatedMetrics` and `EvaluationMessage`**

Each rule result contains the raw metric value that was compared to the threshold. For the
failing `systolic_bp` rule, the result JSON looks like:

```json
{
  "Name": "Rule_12",
  "Description": "ColumnValues \"systolic_bp\" between 50 and 250",
  "Result": "FAIL",
  "EvaluationMessage": "Value: 461.0 does not meet the constraint requirement!\nValue: NULL does not meet the constraint requirement!",
  "EvaluatedMetrics": {
    "Column.systolic_bp.Maximum": 461.0,
    "Column.systolic_bp.Minimum": 52.0
  }
}
```

**3. S3 Results** — Written automatically to the `ResultsS3Prefix` configured above,
partitioned by table name and date:

```
s3://demo-1-healthcare-records-pdx/dq-results/
├── patient_records_csv/
│   └── year=2026/month=03/day=14/
│       └── <run-id>.jsonl    ← one JSON line per rule
├── patient_records_json/
│   └── year=2026/month=03/day=14/
│       └── <run-id>.jsonl
└── clinical_notes_json/
    └── year=2026/month=03/day=14/
        └── <run-id>.jsonl
```

Each line in the JSONL file contains: `catalogId`, `databaseName`, `tableName`, `dqRunId`,
`rule`, `outcome`, `failureReason`, `evaluatedMetrics`. This S3 output enables historical
DQ trend analysis via Athena queries or QuickSight dashboards over time, with results
queryable across all three tables from a single Athena table (partition by `tableName`).

**4. CloudWatch Metrics** — Published automatically to namespace `"Glue Data Quality"` when
`CloudWatchMetricsEnabled: true` (set in `AdditionalRunOptions`):

| Metric Name | Dimensions | Values from Our 3 Runs |
|-------------|-----------|------------------------|
| `glue.data.quality.rules.passed` | Ruleset, Table, Database, CatalogId | csv: 15, json: 20, notes: 14 |
| `glue.data.quality.rules.failed` | Ruleset, Table, Database, CatalogId | csv: 4, json: 3, notes: 0 |

Each table's evaluation publishes its own metrics tagged with the `Table` dimension
(`patient_records_csv`, `patient_records_json`, `clinical_notes_json`) — allowing
per-table CloudWatch alarms and dashboards.

> **Note:** There is no automatic `score` metric — you must calculate it from
> `passed / (passed + failed)` using a CloudWatch Metric Math expression.
> The `CW_NAMESPACE = "AIP-C01/HealthcareDataQuality"` used by `quality_dashboard.py` is a
> separate, custom namespace for Lambda validation metrics. The Glue DQ metrics land in the
> distinct `"Glue Data Quality"` namespace managed by the Glue service itself.

**5. Console visibility** — Results appear immediately after the run completes under:

```
Glue Console → Data Catalog → Tables → patient_records_csv → Data Quality tab
```

The tab shows: overall DQ score, per-run history, per-rule PASS/FAIL breakdown, and (after
enough evaluation runs accumulate) anomaly detection observations from the Analyzers.

**Cost:** ~$0.44/DPU-hour with 2 workers × ~2-minute cluster startup + evaluation =
approximately **$0.03 per evaluation run** — low enough to run on every batch load.

---

#### Step 6c — Rule Recommendations: Auto-Discovering Rules from Data

`start-data-quality-rule-recommendation-run` is the **"Recommend rules"** button from the
Glue console, now scripted as Step 6c. Glue analyzes the actual data distributions and
auto-generates a starting set of DQDL rules — the data engineer does not have to write every
rule from scratch.

**CLI command (from `scripts/setup-aws-infra.sh` — `run_dq_recommendation()`):**

```bash
aws glue start-data-quality-rule-recommendation-run \
    --data-source "{
        \"GlueTable\": {
            \"DatabaseName\": \"demo_1_healthcare_records_pdx\",
            \"TableName\": \"patient_records_csv\"
        }
    }" \
    --role "arn:aws:iam::${AWS_ACCOUNT_ID}:role/AWSGlueServiceRole-HealthRecords" \
    --number-of-workers 2 \
    --timeout 10 \
    --region us-west-2
```

Poll and retrieve the recommended DQDL:

```bash
# Poll until Status = SUCCEEDED
aws glue get-data-quality-rule-recommendation-run \
    --run-id <RunId> \
    --region us-west-2 \
    --query "{Status: Status, RecommendedRuleset: RecommendedRuleset}"
```

#### What recommendations look like

For our 200-row CSV with 17 columns, Glue recommended **69 rules**, a sample:

```
Rules = [
    IsComplete "record_id",
    IsComplete "mrn",
    IsComplete "patient_name",
    IsComplete "age",
    ...
    ColumnValues "age" between 25 and 89 with threshold >= 0.95,
    ColumnValues "systolic_bp" between 98 and 160 with threshold >= 0.95,
    ColumnValues "gender" in ["F", "M"],
    ColumnValues "department" in [
        "Cardiology", "Emergency", "ICU", "Neurology",
        "Oncology", "Orthopedics", "Pediatrics", "Pulmonology"
    ],
    StandardDeviation "age" between 15.0 and 25.0,
    ColumnLength "primary_icd10" between 3 and 7 with threshold >= 0.99,
    RowCount between 100 and 300
]
```

Rule recommendations reflect actual data distributions — the `between 25 and 89` for age
reflects what Glue observed in the synthetic dataset, not the true clinical requirement.
The data engineer's job after receiving recommendations:

1. **Review** — understand what each rule captures
2. **Tighten thresholds** — e.g., `between 0 and 120` instead of `25 and 89` to match clinical reality
3. **Remove noise** — drop rules for columns without business-critical quality requirements
4. **Add domain rules** — data distributions alone cannot reveal logical constraints like
   `discharge_date >= admission_date` or ICD-10 code format

#### How recommendations connect to Section 2

This scripted step is the production workflow described in
[Section 2 — Who Creates DQ Rulesets?](#2-who-creates-dq-rulesets). The auto-recommend
output gives the data engineer a starting point especially valuable for wide tables (17+
columns) where writing every completeness and range rule by hand is tedious. In this demo,
our 19 hand-crafted rules cover business requirements (clinical vital-sign ranges, ICD-10
format, temporal consistency) that the automated recommendations alone would not capture.

> **Caching:** Step 6c checks `list-data-quality-rule-recommendation-runs` first and skips
> a new run if a prior recommendation exists for the same table. Recommendation runs
> auto-delete after 90 days. The `--created-ruleset-name` optional parameter on
> `start-data-quality-rule-recommendation-run` can auto-create a named ruleset directly
> from the recommendation output — useful for rapid onboarding of new tables.

---

## 4. The Dependency Chain Diagram

The six-step build-up has a strict sequential dependency — each layer only
works if the layer below it exists.

```
┌─────────────────────────────────────────────────────────────────────────┐
│  STEP 1–2: DATA IN S3                                                   │
│                                                                         │
│  synth_data/output/                                                     │
│  ├── patient_records.csv   ──────────────────────────────────────────┐  │
│  ├── patient_records.json  ──────────────────────────────────────┐   │  │
│  └── clinical_notes.json   ──────────────────────────────────┐   │   │  │
│                            aws s3 sync                       │   │   │  │
│                                ↓                             │   │   │  │
│  s3://demo-1-healthcare-records-pdx/raw-data/  ◄─────────────┘───┘───┘  │
└─────────────────────────────────────────────────────────────────────────┘
                   │
                   │  Data is in S3 but invisible to analytics
                   ↓
┌─────────────────────────────────────────────────────────────────────────┐
│  STEPS 3–4: IAM ROLE + GLUE DATABASE                                    │
│                                                                         │
│  IAM Role: AWSGlueServiceRole-HealthRecords                             │
│    • AWSGlueServiceRole managed policy                                  │
│    • S3 read on demo-1-healthcare-records-pdx                           │
│    • lakeformation:GetDataAccess (credential vending)                   │
│                                                                         │
│  Glue Database: demo_1_healthcare_records_pdx (empty — no tables yet)   │
└─────────────────────────────────────────────────────────────────────────┘
                   │
                   │  Database exists — Lake Formation grants can be set
                   ↓
┌─────────────────────────────────────────────────────────────────────────┐
│  STEP 4b: LAKE FORMATION PERMISSIONS  ← REQUIRED IN MODERN AWS ACCOUNTS │
│                                                                         │
│  DATABASE-level (for Glue role):                                        │
│    • CREATE_TABLE, DESCRIBE, ALTER on demo_1_healthcare_records_pdx     │
│                                                                         │
│  TABLE wildcard (for Glue role AND deployer role):                      │
│    • ALL, SELECT, ALTER, DROP, DELETE, INSERT, DESCRIBE                 │
│    • {"Table": {"DatabaseName": "...", "TableWildcard": {}}}            │
│                                                                         │
│  Without this step: crawler reports SUCCEEDED but tables are invisible  │
│  (get-tables returns [] — Lake Formation silently filters them out)     │
└─────────────────────────────────────────────────────────────────────────┘
                   │
                   │  LF permissions in place — crawler can create visible tables
                   ↓
┌─────────────────────────────────────────────────────────────────────────┐
│  STEP 5: GLUE CRAWLER                                                   │
│                                                                         │
│  demo-1-healthcarerecords-pdx-crawler                                   │
│    • Reads CSV/JSON files from raw-data/                                │
│    • Infers schema (column names, data types)                           │
│    • Creates table entry in Glue Data Catalog                           │
│    • Table name is AUTO-GENERATED from file name                        │
│        patient_records.csv → "patient_records_csv"                      │
│                                                                         │
│  Post-crawler: script discovers actual table name via get-tables        │
│    DISCOVERED_TABLE_NAME = "patient_records_csv"                        │
└─────────────────────────────────────────────────────────────────────────┘
                   │
                   │  S3 data is now registered as a queryable table
                   ↓
┌─────────────────────────────────────────────────────────────────────────┐
│  GLUE DATA CATALOG                                                      │
│                                                                         │
│  Database: demo_1_healthcare_records_pdx                                │
│  └── Table: patient_records_csv                                         │
│        Location  → s3://demo-1-healthcare-records-pdx/raw-data/         │
│        Columns   → record_id (bigint), mrn (string), age (bigint), ...  │
│                                                                         │
│       [ Athena, Glue ETL, and Glue DQ can now query these tables ]      │
└─────────────────────────────────────────────────────────────────────────┘
                   │
                   │  Tables exist — rulesets can be attached
                   ↓
┌─────────────────────────────────────────────────────────────────────────┐
│  STEP 6: GLUE DQ RULESETS (3 definitions — no data read yet)            │
│                                                                         │
│  ① healthcare-patient-records-csv-dq                                    │
│      → demo_1_healthcare_records_pdx.patient_records_csv                │
│      Source: glue_dq_ruleset_patient_records_csv.dqdl                   │
│      19 rules (flat CSV) + 10 Analyzers                                 │
│                                                                         │
│  ② healthcare-patient-records-json-dq                                   │
│      → demo_1_healthcare_records_pdx.patient_records_json               │
│      Source: glue_dq_ruleset_patient_records_json.dqdl                  │
│      23 rules (nested JSON — CustomSql for struct/array) + 8 Analyzers  │
│                                                                         │
│  ③ healthcare-clinical-notes-json-dq                                    │
│      → demo_1_healthcare_records_pdx.clinical_notes_json                │
│      Source: glue_dq_ruleset_clinical_notes_json.dqdl                   │
│      14 rules (text quality, format regex) + 7 Analyzers                │
└─────────────────────────────────────────────────────────────────────────┘
                   │
                   │  Rulesets registered — two serverless runs available
                   ├─────────────────────────────────────────────────────┐
                   │  (6b — evaluate all 3)         (6c — recommend)     │
                   ↓                                ↓
┌──────────────────────────────────┐  ┌──────────────────────────────────┐
│  STEP 6b: EVALUATION RUNS        │  │  STEP 6c: RULE RECOMMENDATION    │
│  (loop over all 3 tables)        │  │  (optional — run once to seed)   │
│                                  │  │                                  │
│  Serverless Spark cluster (2 DPU)│  │  Glue analyzes data distributions│
│  → evaluates each table's rules  │  │  → auto-generates DQDL rules     │
│  → produces Score + per-rule     │  │  → for our dataset: 69 rules     │
│    PASS/FAIL + EvaluatedMetrics  │  │                                  │
│  → AnalyzerResults collected     │  │  RecommendedRuleset:             │
│    for anomaly detection profile │  │    IsComplete, ColumnValues,     │
│                                  │  │    StandardDeviation,            │
│  csv:   79.0% (15/19 passed)     │  │    ColumnLength, RowCount…       │
│  json:  87.0% (20/23 passed)     │  │  → Data engineer reviews +       │
│  notes: 100%  (14/14 passed)     │  │    tightens thresholds           │
└──────────────────────────────────┘  └──────────────────────────────────┘
                   │
                   │  Results flow to four destinations (per table)
                   ├────────────────────────────────────────────────────────┐
                   ↓                         ↓                    ↓         ↓
┌──────────────┐  ┌───────────────────┐  ┌────────────────┐  ┌───────────────┐
│  GLUE        │  │  S3 RESULTS       │  │  CLOUDWATCH    │  │  CONSOLE      │
│  CONSOLE     │  │                   │  │  METRICS       │  │  URL          │
│              │  │  dq-results/      │  │                │  │               │
│  Table →     │  │  <table_name>/    │  │  Namespace:    │  │  table/view/  │
│  Data Quality│  │  year=YYYY/       │  │  "Glue Data    │  │  per table    │
│  tab         │  │  month=MM/        │  │   Quality"     │  │  ?tab=        │
│              │  │  day=DD/          │  │                │  │  dataQuality  │
│  Score trend │  │  <run-id>.jsonl   │  │  • rules.passed│  │               │
│  per-run hist│  │                   │  │  • rules.failed│  │  Score visible│
│  rule results│  │  1 line / rule    │  │  (per Table    │  │  immediately  │
│  observations│  │  + failureReason  │  │   dimension)   │  │  after run    │
│              │  │  + evalMetrics    │  │                │  │  completes    │
│  Anomaly     │  │                   │  │  → CloudWatch  │  │               │
│  detection   │  │  → Athena queries │  │    alarms +    │  │               │
│  trends      │  │  → QuickSight     │  │    SNS alerts  │  │               │
└──────────────┘  └───────────────────┘  └────────────────┘  └───────────────┘
                   │
                   ↓
┌─────────────────────────────────────────────────────────────────────────┐
│  QUALITY REPORT (AWS Evaluation Runs — actual results)                  │
│                                                                         │
│  patient_records_csv:   200 rows | 19 rules | 15 pass | 4 fail | 79.0% │
│  patient_records_json:  200 rows | 23 rules | 20 pass | 3 fail | 87.0% │
│  clinical_notes_json:   200 rows | 14 rules | 14 pass | 0 fail | 100%  │
│                                                                         │
│  Downstream gate: only send records that pass DQ to Lambda / Bedrock    │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## 5. DQDL Rule Reference

DQDL (Data Quality Definition Language) is AWS Glue's declarative rule syntax.
Here is the complete ruleset used for the primary table (`patient_records_csv`),
with every rule annotated. See the `.dqdl` files in `glue_quality/output/` for
the JSON-specific rulesets.

```
Rules = [
    # ── COMPLETENESS ──────────────────────────────────────────────────────
    # Fraction of rows where the column is non-null and non-empty.
    # 1.0 = every single row must have a value.
    # 0.98 = up to 2% null/blank is tolerated.
    Completeness "patient_name" >= 0.98,
    Completeness "mrn" = 1.0,
    Completeness "admission_date" = 1.0,
    Completeness "discharge_date" = 1.0,
    Completeness "primary_icd10" >= 0.95,
    Completeness "systolic_bp" >= 0.90,
    Completeness "diastolic_bp" >= 0.90,

    # ── UNIQUENESS ────────────────────────────────────────────────────────
    # Ratio of distinct values to total non-null values.
    # 1.0 = every value must be unique (no duplicates).
    Uniqueness "record_id" = 1.0,

    # ── CONSISTENCY (CustomSql) ───────────────────────────────────────────
    # Arbitrary SQL run against the table. Result must equal 0 (zero bad rows).
    # "primary" is the alias for the target table in DQDL context.
    CustomSql "SELECT COUNT(*) FROM primary WHERE discharge_date < admission_date" = 0,

    # ── RANGE / COLUMN VALUES ─────────────────────────────────────────────
    # All non-null values must fall within the specified range.
    ColumnValues "length_of_stay_days" >= 0,
    ColumnValues "age" between 0 and 120,
    ColumnValues "systolic_bp" between 50 and 250,
    ColumnValues "diastolic_bp" between 30 and 150,
    ColumnValues "heart_rate" between 20 and 250,
    ColumnValues "temperature_f" between 90.0 and 108.0,

    # ── FORMAT (regex match) ──────────────────────────────────────────────
    # Fraction of rows matching the regex must meet the threshold.
    # ICD-10: one uppercase letter + 2 digits + optional .1 or .2 digit suffix
    ColumnValues "primary_icd10" matches "[A-Z][0-9]{2}(\.[0-9]{1,2})?"
        with threshold >= 0.90,

    # ── STATISTICAL ───────────────────────────────────────────────────────
    # Aggregate checks across all rows.
    Mean "age" between 40 and 80,
    StandardDeviation "length_of_stay_days" <= 15,

    # ── ROW COUNT ─────────────────────────────────────────────────────────
    # Total row count must meet a minimum floor.
    RowCount >= 100
]

# ── ANALYZERS ─────────────────────────────────────────────────────────────
# Analyzers collect statistics WITHOUT producing a pass/fail result.
# They are separate from Rules and run in parallel during every evaluation.
# Over multiple runs, Glue builds a statistical profile that powers
# anomaly detection — it learns what "normal" looks like and flags
# deviations as Observations in subsequent evaluation results.
#
# Source: glue_quality/output/glue_dq_ruleset.dqdl
# (Added by glue-deep-research agent — not yet back-ported to glue_dq_rules.py
#  GLUE_DQ_RULESET string; the .dqdl file is the deployed source of truth.)

Analyzers = [
    # Volume anomaly detection — alert if row count drops or spikes
    RowCount,

    # Completeness trend tracking for critical fields
    Completeness "patient_name",
    Completeness "mrn",
    Completeness "primary_icd10",

    # Column length statistics for format drift detection
    ColumnLength "primary_icd10",

    # Distinct value tracking — detect new values entering the dataset
    DistinctValuesCount "disposition",
    DistinctValuesCount "primary_icd10",

    # Distribution statistics for vital signs — detect patient population drift
    Mean "age",
    Mean "systolic_bp",
    StandardDeviation "heart_rate"
]
```

### Rule category summary

> **Note:** The summary below describes `patient_records_csv` (the primary flat-table
> ruleset with 19 rules + 10 analyzers). All three rulesets are summarized in the
> table at the top of [Step 6](#step-6-the-glue-dq-rulesets-multi-table).

| Category | Rules | What it catches |
|----------|-------|----------------|
| **Completeness** | 7 | Missing / null critical fields |
| **Uniqueness** | 1 | Duplicate record IDs |
| **Consistency** | 1 | Discharge before admission (logical impossibility) |
| **Range** | 6 | Out-of-physiological-range vitals, negative ages, impossible LOS |
| **Format** | 1 | Malformed ICD-10 codes |
| **Statistical** | 2 | Population-level anomalies (age distribution, LOS spread) |
| **Row Count** | 1 | Truncated or empty dataset |
| **Total Rules** | **19** | (CSV ruleset only) |
| | | |
| **Analyzers** | 10 | Statistics only — no pass/fail. Feed anomaly detection model over time. |

### JSON-specific DQDL techniques (patient_records_json and clinical_notes_json)

The JSON rulesets require different approaches because Glue's built-in DQDL rules have
limited support for nested data types. These techniques are documented in
[AWS_AI_LEARNINGS.md entries #14, #15, #16].

#### Nested struct/array access — CustomSql instead of Completeness

> **⚠️ From [AWS_AI_LEARNINGS.md entry #15]:** `Completeness` does NOT work on
> `array<struct<...>>` or `struct<...>` column types. Applying it produces an
> "Unsupported nested column type" error that counts as a rule failure.

The JSON tables have nested columns (`diagnoses` as `array<struct<...>>`,
`vitals_at_discharge` as `struct<...>`). Use `CustomSql IS NULL` checks instead:

```dqdl
# Instead of (FAILS with unsupported type error):
#   Completeness "diagnoses" >= 0.95

# Use CustomSql IS NULL checks:
CustomSql "SELECT COUNT(*) FROM primary WHERE diagnoses IS NULL" <= 10
CustomSql "SELECT COUNT(*) FROM primary WHERE medications IS NULL" <= 20
CustomSql "SELECT COUNT(*) FROM primary WHERE vitals_at_discharge IS NULL" <= 20

# Array emptiness (non-null but empty []):
CustomSql "SELECT COUNT(*) FROM primary WHERE size(diagnoses) = 0" = 0

# Nested struct field access (dot notation):
CustomSql "SELECT COUNT(*) FROM primary WHERE vitals_at_discharge.systolic_bp > 250" = 0
```

**What works for nested types in DQDL:**

| Feature | Nested column support | Alternative |
|---------|-----------------------|-------------|
| `Completeness` | ❌ No | `CustomSql IS NULL` |
| `ColumnValues` | ❌ No | `CustomSql` with dot notation |
| `CustomSql` | ✅ Yes | — (primary tool) |
| `size()` in CustomSql | ✅ Yes | Array length check |
| Dot notation in CustomSql | ✅ Yes | `struct.field` access |
| `Analyzers` (Completeness) | ❌ No | Omit from Analyzers section |

#### Regex escaping in .dqdl files

> **⚠️ From [AWS_AI_LEARNINGS.md entry #16]:** The `.dqdl` file uses Java regex directly.
> Use **single backslash** for escape sequences in the file. In Python triple-quoted
> strings, `"\\."`  produces `\.` in the file (correct for literal dot in Java regex).
> Using `"\\\\."` would produce `\\.` in the file (a literal backslash, which is wrong).

```dqdl
# In the .dqdl file (correct — literal dot in Java regex):
ColumnValues "author" matches "Dr\. [A-Z][a-z]+" with threshold >= 0.90

# In Python that generates the .dqdl file:
pattern = "Dr\\. [A-Z][a-z]+"   # Python's \\ → single \ in output file → \. in Java regex
```

### The NULL-as-failure trap for range rules

> **⚠️ From [AWS_AI_LEARNINGS.md entry #13]:** `ColumnValues` range rules evaluate NULL
> values as failures, even when NULLs are separately covered by a `Completeness` rule.
>
> In this demo, both `systolic_bp` and `diastolic_bp` have ~0.5% NULL rows. The
> `Completeness "systolic_bp" >= 0.90` rule **passes** (0.5% ≪ 10% threshold), but
> `ColumnValues "systolic_bp" between 50 and 250` **fails** those same NULL rows.
>
> Two production-ready fix patterns:
>
> **Option A — Threshold on the ColumnValues rule** (simple, allows the known NULL fraction):
> ```
> ColumnValues "systolic_bp" between 50 and 250 with threshold >= 0.95
> ```
>
> **Option B — CustomSql to exclude NULLs** (precise, only counts actual range violations):
> ```
> CustomSql "SELECT COUNT(*) FROM primary
>   WHERE systolic_bp IS NOT NULL
>   AND (systolic_bp < 50 OR systolic_bp > 250)" = 0
> ```
>
> The trade-off: Option A is concise but the threshold is coupled to the NULL rate and will
> pass if outliers stay under 5%. Option B is exact but more verbose. For clinical data where
> any out-of-range vital sign should be a hard failure, Option B is the safer choice.

---

## 6. Anomaly Detection & Analyzers

### What Analyzers are (and what they are not)

Analyzers are **statistics collectors** — they run during every evaluation alongside the
Rules but produce **no pass/fail judgment**. Think of them as the telemetry layer that feeds
the quality model over time. While Rules enforce hard constraints ("age must be between 0 and
120"), Analyzers track continuous metrics ("what is the mean age this week vs. last week?").

```
Rules   → produce: PASS / FAIL / ERROR  ← hard gates
Analyzers → produce: computed statistics ← training data for the anomaly model
```

Results from Analyzers appear in two places in the `get-data-quality-result` response:
- `AnalyzerResults` — the raw computed value for each analyzer at each run
- `Observations` — anomaly alerts once Glue has enough history to define "normal"

### The 10 analyzers in this demo

From `glue_quality/output/glue_dq_ruleset.dqdl`:

| Analyzer | Type | What it tracks |
|----------|------|---------------|
| `RowCount` | Volume | Total rows — detects truncated loads, runaway data |
| `Completeness "patient_name"` | Completeness | Fraction of non-null patient names over time |
| `Completeness "mrn"` | Completeness | MRN fill rate trend |
| `Completeness "primary_icd10"` | Completeness | ICD-10 fill rate trend |
| `ColumnLength "primary_icd10"` | Length stats | Min/max/avg length of ICD-10 codes — detects format drift |
| `DistinctValuesCount "disposition"` | Cardinality | Number of distinct discharge dispositions — detects new/removed categories |
| `DistinctValuesCount "primary_icd10"` | Cardinality | Distinct ICD-10 code count — detect unexplained new codes |
| `Mean "age"` | Distribution | Patient population age drift |
| `Mean "systolic_bp"` | Distribution | Vital sign distribution shift |
| `StandardDeviation "heart_rate"` | Distribution | Spread of heart rate — detects sensor calibration issues |

### How the anomaly detection model builds up

Glue DQ anomaly detection is **not a feature you turn on** — it builds automatically as
evaluation runs accumulate. The process:

```
Run 1   → AnalyzerResults collected, stored in Glue service
Run 2   → Second data point added to profile
Run 3   → Third data point
Run 4   → Fourth data point
Run 5+  → Glue trains a lightweight statistical model
              (expected value + upper/lower bounds for each analyzer)
              → If new run's value falls outside bounds: Observation is generated
              → Observation appears in get-data-quality-result → Observations[]
```

The minimum number of runs before Glue starts generating anomaly observations is
approximately **5 evaluation runs**. In production, weekly or nightly runs build the
baseline within 1–2 weeks.

### Reading Observations in the result

Once enough history exists, `get-data-quality-result` returns an `Observations` array.
Each observation has a `MetricBasedObservation` with:

```json
{
  "Observations": [
    {
      "Description": "An anomaly was detected for metric Column.age.Mean",
      "MetricBasedObservation": {
        "MetricName": "Column.age.Mean",
        "MetricValues": {
          "ActualValue": 72.3,
          "ExpectedValue": 55.4,
          "LowerLimit": 45.2,
          "UpperLimit": 65.6
        }
      }
    }
  ]
}
```

This observation says: "the mean patient age in this batch (72.3) is outside the expected
range (45.2–65.6) based on historical runs." A spike like this could mean:
- The batch contains only ICU/elderly patients (sampling bias)
- A data entry error shifted ages systematically
- A legitimate population shift in the patient mix

The Observations do **not** cause the evaluation run to FAIL — they are informational.
Your pipeline should inspect `len(result.get("Observations", []))` and route high-observation
batches to a human review queue.

### Console location for anomaly trends

After enough runs accumulate:
```
Glue Console → Data Catalog → Tables → patient_records_csv
→ Data Quality tab → Anomaly Detection section
```

The tab shows time-series charts for each tracked metric with the expected bounds
highlighted — visually identifying which runs were anomalous without needing to call the API.

### Implementation note — Analyzers in the .dqdl file vs Python source

> **⚠️ NOTE:** The `GLUE_DQ_RULESET` string in `glue_quality/glue_dq_rules.py` does **not**
> currently include the `Analyzers = [...]` block. The 10 Analyzers were added directly to
> `glue_quality/output/glue_dq_ruleset.dqdl` by the glue-deep-research agent (2026-03-14).
> The `.dqdl` file is the authoritative deployed version. To make the Python source fully
> consistent, `GLUE_DQ_RULESET` in `glue_dq_rules.py` should be updated to include the
> Analyzers block, so that re-running `python3 glue_quality/glue_dq_rules.py` regenerates
> the `.dqdl` with the Analyzers intact.

---

## 7. Local vs. AWS Execution

The same logical rules are expressed in two ways:

### Python simulation (local, no AWS needed)

`glue_quality/glue_dq_rules.py` reads the CSV directly and replicates the
rule logic in pure Python:

```python
# Completeness — pure Python equivalent of DQDL Completeness
def check_completeness(records, column, threshold):
    total = len(records)
    non_null = sum(1 for r in records if r.get(column) not in [None, "", "None"])
    actual = non_null / total if total > 0 else 0
    return {
        "rule": f'Completeness "{column}" >= {threshold}',
        "passed": actual >= threshold,
        "actual": round(actual, 4),
    }

# Range — pure Python equivalent of DQDL ColumnValues between
def check_column_range(records, column, min_val, max_val):
    violations = [r for r in records
                  if r.get(column) is not None
                  and (r[column] < min_val or r[column] > max_val)]
    return {
        "rule": f'ColumnValues "{column}" between {min_val} and {max_val}',
        "passed": len(violations) == 0,
    }

# Consistency — pure Python equivalent of DQDL CustomSql
def check_date_consistency(records):
    violations = [r for r in records
                  if datetime.strptime(r["discharge_date"], "%Y-%m-%d")
                   < datetime.strptime(r["admission_date"], "%Y-%m-%d")]
    return {"rule": "CustomSql: discharge_date >= admission_date",
            "passed": len(violations) == 0}
```

Run locally (no AWS credentials required):

```bash
# From demo-1-healthcare-records/
python3 synth_data/generate_healthcare_data.py   # generate data first
python3 glue_quality/glue_dq_rules.py            # run DQ simulation
```

Outputs written to `glue_quality/output/`:
- `glue_dq_report.json` — full pass/fail report
- `glue_dq_ruleset.dqdl` — ready for upload to AWS
- `glue_dq_api_example.json` — boto3 API call reference

### AWS managed evaluation (real infrastructure)

The bash script runs the same ruleset in AWS via `setup-aws-infra.sh`. The
DQDL file generated locally is uploaded verbatim — the rule text is identical
in both paths.

```
Local path:                              AWS path:
──────────────────────────────           ───────────────────────────────────────
glue_dq_rules.py                         glue_dq_ruleset.dqdl (same text)
  reads CSV directly                →      uploaded to Glue Data Catalog
  Python functions simulate rules          Glue managed job evaluates rules
  writes glue_dq_report.json              results in Glue DQ console + API
  (immediate, no infra needed)            (requires Steps 1–5 to be complete)
```

**Why this matters for exam prep:** The local path lets you inspect rule
behavior on your laptop, understand pass/fail logic, and tune thresholds before
you ever touch a real AWS account. The AWS path demonstrates the actual service
integration being tested on the AIP-C01 exam.

> **⚠️ AWS gotcha — the invisible table trap:** When running against real AWS infrastructure,
> `aws glue get-tables` can silently return an empty list even after a successful crawler run.
> This happens when Lake Formation TABLE-level permissions are missing (see Step 4b). The
> tables physically exist in the catalog, but Lake Formation filters them from `get-tables`
> output without any error message. The only diagnostic signal is to call `get-table` by the
> exact name — if the table exists-but-is-blocked you get `AccessDeniedException`; if it truly
> doesn't exist you get `EntityNotFoundException`. When `get-tables` returns empty, Step 6
> cannot discover the table name and falls back to creating the ruleset without `--target-table`.
> The fix is ensuring Step 4b's TABLE wildcard grants are applied before the crawler runs.
> See [AWS_AI_LEARNINGS.md entry #11] for the complete diagnosis and fix.

---

## 8. Reading the Quality Report

After running `glue_dq_rules.py` locally, `glue_quality/output/glue_dq_report.json`
contains the full simulation results. Here is the actual report produced from
the 200-record synthetic dataset (generated 2026-03-14):

```json
{
  "report_timestamp": "2026-03-14T10:48:12.684567",
  "dataset": "patient_records.csv",
  "total_records": 200,
  "total_rules": 17,
  "rules_passed": 13,
  "rules_failed": 4,
  "overall_score": 76.5
}
```

### Rule-by-rule results

| # | Rule | Result | Detail |
|---|------|--------|--------|
| 1 | `Completeness "patient_name" >= 0.98` | ❌ FAIL | Actual: 0.975 — 5 records have blank name (`introduce_issues` → `missing_name`) |
| 2 | `Completeness "mrn" = 1.0` | ✅ PASS | Actual: 1.0 — all 200 MRNs present |
| 3 | `Completeness "admission_date" = 1.0` | ✅ PASS | Actual: 1.0 |
| 4 | `Completeness "discharge_date" = 1.0` | ✅ PASS | Actual: 1.0 |
| 5 | `Completeness "primary_icd10" >= 0.95` | ✅ PASS | Actual: 0.995 |
| 6 | `Completeness "systolic_bp" >= 0.90` | ✅ PASS | Actual: 0.995 |
| 7 | `Completeness "diastolic_bp" >= 0.90` | ✅ PASS | Actual: 0.995 |
| 8 | `Uniqueness "record_id" = 1.0` | ✅ PASS | 200 distinct values / 200 records |
| 9 | `CustomSql: discharge_date >= admission_date` | ✅ PASS | 0 violations (LOS always ≥ 1 day by construction) |
| 10 | `ColumnValues "age" between 0 and 120` | ❌ FAIL | 3 violations: records 94, 137, 190 have `age = -5` |
| 11 | `ColumnValues "systolic_bp" between 50 and 250` | ❌ FAIL | 3 violations: records 30 (461), 157 (431), 190 (374) |
| 12 | `ColumnValues "diastolic_bp" between 30 and 150` | ✅ PASS | 0 violations |
| 13 | `ColumnValues "heart_rate" between 20 and 250` | ✅ PASS | 0 violations |
| 14 | `ColumnValues "temperature_f" between 90.0 and 108.0` | ❌ FAIL | 3 violations: records 72 (114.1°F), 79 (113.7°F), 137 (112.0°F) |
| 15 | `ColumnValues "length_of_stay_days" between 0 and 100` | ✅ PASS | 0 violations |
| 16 | `ColumnValues "primary_icd10" matches ICD-10 pattern` | ✅ PASS | 199/199 valid — ratio 1.0 |
| 17 | `RowCount >= 100` | ✅ PASS | Actual: 200 |

### Why exactly these records fail

The synthetic data generator introduces errors at a 12% rate:

```python
# From synth_data/generate_healthcare_data.py
num_records = 200
error_rate = 0.12   # 12% of records will have intentional issues
```

The specific failure modes that triggered these four rule failures:

| Error type | Code in generator | Rule violated |
|-----------|------------------|---------------|
| `"negative_age"` | `record["age"] = -5` | `ColumnValues "age" between 0 and 120` |
| `"impossible_bp"` | `vitals["systolic_bp"] = random.randint(300, 500)` | `ColumnValues "systolic_bp" between 50 and 250` |
| `"extreme_temp"` | `vitals["temperature_f"] = round(random.uniform(110.0, 120.0), 1)` | `ColumnValues "temperature_f" between 90.0 and 108.0` |
| `"missing_name"` | `record["patient_name"] = ""` | `Completeness "patient_name" >= 0.98` |

This is by design — the dataset is built to demonstrate Glue DQ catching real
problems, not to represent a clean dataset.

---

## 9. Production Usage Patterns

In a production healthcare data pipeline, Glue DQ fits into four operational
patterns:

### Pattern A — Scheduled batch evaluation

```
EventBridge rule (cron: rate(1 day) or cron(0 2 * * ? *))
    → triggers start-data-quality-ruleset-evaluation-run
    → Glue reads overnight batch from S3 via catalog (patient_records_csv)
    → CloudWatch auto-publishes rules.passed / rules.failed to "Glue Data Quality" namespace
    → CloudWatch alarm: if rules.failed > threshold → SNS topic → PagerDuty / Slack
    → Downstream ETL/Bedrock job blocked or warned based on score
    → S3 results land in dq-results/year=.../month=.../day=.../ for historical trending
```

**Anomaly detection builds over time:** Each scheduled run adds an `AnalyzerResults`
data point to the Glue statistical profile. After ~5 runs (~5 nights), Glue starts
generating `Observations` for metric values outside learned bounds — detecting drift
without requiring you to write explicit threshold rules.

### Pattern B — Event-driven on new data

```
Patient EHR system
    → uploads new discharge batch to s3://…/raw-data/
    → S3 EventNotification → Lambda trigger
    → Lambda calls start-data-quality-ruleset-evaluation-run
    → Quality gate: poll get-data-quality-ruleset-evaluation-run until SUCCEEDED
    → if Score >= 0.90 AND len(Observations) == 0:
          → send batch to downstream Bedrock pipeline
    → if Score < 0.90 OR len(Observations) > 0:
          → quarantine to s3://…/quarantine/ + SNS alert for human review
```

**EventBridge Pipes integration (future enhancement):** Instead of a Lambda poller, use
EventBridge Pipes to react to the Glue DQ run completion event directly — Glue publishes
`DataQuality.RulesetEvaluationRun.Completed` to EventBridge when a run finishes. The Pipe
can route to SNS, SQS, or Step Functions without a custom Lambda.

### Pattern C — Glue ETL job integration (inline)

```
Glue ETL job (PySpark)
    → reads patient_records_csv table
    → EvaluateDataQuality transform applied inline (uses same DQDL ruleset)
    → Good records (DQ outcome = Passed) → processed/ prefix
    → Bad records  (DQ outcome = Failed) → rejected/ prefix (with rule violation tags)
    → Lambda picks up processed/ for Bedrock API calls
```

The `EvaluateDataQuality` PySpark transform runs the DQDL rules **in-process** during the
ETL job — no separate evaluation run needed. Each output record is tagged with which rules
it passed or failed, enabling per-record routing.

### Pattern D — Rule recommendation during table onboarding (one-time)

```
New table registered in Glue catalog
    → Run start-data-quality-rule-recommendation-run (Step 6c)
    → Glue analyzes data → returns ~50–70 candidate DQDL rules
    → Data engineer reviews recommendations:
          → accepts sensible completeness and range rules
          → tightens thresholds to match business requirements
          → adds domain rules (date logic, format regex, cross-column checks)
          → saves final ruleset to .dqdl file → Step 6 creates it in AWS
    → Subsequent evaluation runs validate every batch automatically
```

### Connecting back to this demo

```
Demo 1 pipeline (as built):

  all 3 tables (csv / json / clinical_notes)
       ↓ (Step 6b — Glue DQ evaluation runs — 3 tables, 56 total rules)
       │   patient_records_csv:  79.0% (15/19)
       │   patient_records_json: 87.0% (20/23)
       │   clinical_notes_json:  100%  (14/14)
  Score >= threshold per table (e.g., 90%)?
       ├── YES → Lambda clinical_validator.py
       │           ↓
       │           ┌──────────────────────────────────────────────┐
       │           │  RESPONSIBLE AI PRE-FM GATES (5 checks)      │
       │           │  (a) Profanity / inappropriate language      │
       │           │  (b) PHI exposure risk (SSN, email, phone)   │
       │           │  (c) Clinical relevance score (0–100)        │
       │           │  (d) Bias indicator detection                │
       │           │  (e) Content coherence check                 │
       │           └──────────────────────────────────────────────┘
       │           ↓
       │           ICD-10 validation + drug interactions + vitals
       │           ↓
       │           ┌──────────────────────────────────────────────┐
       │           │  CLOUDWATCH METRICS (15 metrics published)   │
       │           │  Namespace: AIP-C01/HealthcareDataQuality    │
       │           │  Validation_QualityScore, PHIExposureRisk,   │
       │           │  ProfanityDetected, ClinicalRelevanceScore…  │
       │           └──────────────────────────────────────────────┘
       │           ↓
       │           Profanity detected? → BLOCKED (never reaches FM)
       │           PHI risk? → Route through Comprehend Medical first
       │           Clean records → format_for_bedrock.py
       │               → Bedrock Claude model call
       └── NO  → flagged_records.json
                   → CloudWatch alarm: Healthcare-DQ-OverallScore-Low
                   → human review queue
```

Glue DQ is the **first gate** — structural rules (missing MRNs, impossible vital signs,
logical date reversals). The Lambda Responsible AI checks are the **second gate** —
content-level safety checks on clinical text before it reaches a foundation model. Records
that fail either gate never reach Bedrock, keeping AI inference costs low and model inputs
clean. Glue DQ Analyzers run silently in the background at each evaluation, building the
statistical baseline that generates anomaly observations once ~5 runs have accumulated.

---

*All resource names, CLI commands, DQDL rules, and report figures in this
document are sourced directly from:*
- *`scripts/setup-aws-infra.sh`*
- *`glue_quality/glue_dq_rules.py`*
- *`glue_quality/output/glue_dq_ruleset_patient_records_csv.dqdl`*
- *`glue_quality/output/glue_dq_ruleset_patient_records_json.dqdl`*
- *`glue_quality/output/glue_dq_ruleset_clinical_notes_json.dqdl`*
- *`glue_quality/output/glue_dq_ruleset.dqdl` (legacy single-table reference)*
- *`glue_quality/output/glue_dq_report.json`*
- *`synth_data/generate_healthcare_data.py`*
- *`lambda_validation/clinical_validator.py`*
- *`AWS_AI_LEARNINGS.md` (entries #12, #13, #14, #15, #16)*
