#!/usr/bin/env bash
#
# setup-aws-infra.sh — Provision AWS infrastructure for Demo 1: Healthcare Records Pipeline
#
# Idempotent: safe to run multiple times. Checks existence before creating each resource.
#
# Usage:
#   ./setup-aws-infra.sh              # Create all resources
#   ./setup-aws-infra.sh --dry-run    # Show what would be created
#   ./setup-aws-infra.sh --cleanup    # Tear down all resources
#   ./setup-aws-infra.sh --update     # Update Lambda code + CW dashboard
#   ./setup-aws-infra.sh --help       # Show this help
#
set -euo pipefail

# ═══════════════════════════════════════════════════════════════════════════════
# CONFIGURATION
# ═══════════════════════════════════════════════════════════════════════════════
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEMO_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
PROJECT_DIR="$(cd "${DEMO_DIR}/../.." && pwd)"
PROGRESS_LOG="${DEMO_DIR}/../progress_log.txt"
LOG_FILE="${SCRIPT_DIR}/setup.log"

# AWS
AWS_REGION="${AWS_REGION:-us-west-2}"
AWS_PROFILE="${AWS_PROFILE:-001}"
export AWS_PROFILE
AWS_ACCOUNT_ID=""  # Populated at runtime via STS

# S3
BUCKET_NAME="demo-1-healthcare-records-pdx"

# Glue
GLUE_DB_NAME="demo_1_healthcare_records_pdx"
GLUE_CRAWLER_NAME="demo-1-healthcarerecords-pdx-crawler"
GLUE_CRAWLER_ROLE_NAME="AWSGlueServiceRole-HealthRecords"
GLUE_DQ_RULESET_NAME="healthcare-patient-records-csv-dq"  # Primary CSV ruleset (backward compat)
GLUE_DQ_RULESET_NAME_LEGACY="healthcare-patient-records-dq-ruleset"  # Old name — delete if found
GLUE_TABLE_NAME="raw_data"  # Fallback default — crawler may auto-generate a different name based on S3 path
DISCOVERED_TABLE_NAME=""   # Populated at runtime after crawler completes (Step 5)

# Multi-table DQ configuration — one ruleset per crawler-created table
# Arrays must be kept in sync (same index = same table)
DQ_TABLE_NAMES=("patient_records_csv" "patient_records_json" "clinical_notes_json")
DQ_RULESET_NAMES=("healthcare-patient-records-csv-dq" "healthcare-patient-records-json-dq" "healthcare-clinical-notes-json-dq")
DQ_RULESET_FILES=("glue_dq_ruleset_patient_records_csv.dqdl" "glue_dq_ruleset_patient_records_json.dqdl" "glue_dq_ruleset_clinical_notes_json.dqdl")
DQ_RULESET_DESCS=("DQ rules for patient records (CSV flat)" "DQ rules for patient records (JSON nested)" "DQ rules for clinical notes (JSON text)")

# Lambda
LAMBDA_FUNCTION_NAME="healthcare-clinical-validator"
LAMBDA_ROLE_NAME="LambdaRole-HealthcareValidator"
LAMBDA_RUNTIME="python3.12"
LAMBDA_HANDLER="clinical_validator.lambda_handler"
LAMBDA_MEMORY=512
LAMBDA_TIMEOUT=300

# CloudWatch
CW_NAMESPACE="AIP-C01/HealthcareDataQuality"
CW_DASHBOARD_NAME="Healthcare-Data-Quality"
CW_ALARM_NAMES=(
    "Healthcare-DQ-OverallScore-Low"
    "Healthcare-Validation-HighErrorRate"
    "Healthcare-DrugInteraction-Alert"
)

# Tags
TAG_PROJECT="aip-c01-demos"
TAG_DEMO="healthcare-records"

# Runtime flags
DRY_RUN=false
CLEANUP=false
UPDATE=false

# Counters for summary
CREATED=0
SKIPPED=0
UPDATED=0
DELETED=0
ERRORS=0

# ═══════════════════════════════════════════════════════════════════════════════
# COLOR & LOGGING UTILITIES
# ═══════════════════════════════════════════════════════════════════════════════
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
BOLD='\033[1m'
NC='\033[0m' # No Color

log()      { local msg="[$(date '+%Y-%m-%d %H:%M:%S')] $*"; echo "${msg}" >> "${LOG_FILE}"; }
info()     { echo -e "${BLUE}ℹ${NC}  $*"; log "INFO  $*"; }
success()  { echo -e "${GREEN}✅${NC} $*"; log "OK    $*"; }
skip()     { echo -e "${YELLOW}⏭${NC}  $*"; log "SKIP  $*"; }
warn()     { echo -e "${YELLOW}⚠️${NC}  $*"; log "WARN  $*"; }
error()    { echo -e "${RED}❌${NC} $*" >&2; log "ERROR $*"; }
step()     { echo -e "\n${BOLD}${CYAN}── Step $1: $2${NC}"; log "STEP  $1 — $2"; }
header()   { echo -e "\n${BOLD}════════════════════════════════════════════════════════════${NC}"; echo -e "${BOLD}  $*${NC}"; echo -e "${BOLD}════════════════════════════════════════════════════════════${NC}\n"; }
dryrun()   { echo -e "  ${YELLOW}[DRY-RUN]${NC} Would: $*"; log "DRYRUN $*"; }

progress() {
    local status="$1"; shift
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] [bash-aws-helper] [demo-1-healthcare-records] [${status}] $*" >> "${PROGRESS_LOG}"
}

# Error trap
trap 'error "Script failed at line ${LINENO} (exit code $?)"; progress "FAILED" "Script error at line ${LINENO}"' ERR

# ═══════════════════════════════════════════════════════════════════════════════
# USAGE
# ═══════════════════════════════════════════════════════════════════════════════
usage() {
    cat <<EOF
${BOLD}Usage:${NC} $(basename "$0") [OPTIONS]

Provision AWS infrastructure for Demo 1: Healthcare Records Processing Pipeline.

${BOLD}Options:${NC}
  --dry-run    Show what would be created without executing any AWS commands
  --cleanup    Tear down all resources in reverse order
  --update     Update Lambda function code and CloudWatch dashboard
  --help       Show this help message

${BOLD}Resources created:${NC}
  • S3 bucket:         ${BUCKET_NAME}
  • IAM role (Glue):   ${GLUE_CRAWLER_ROLE_NAME}
  • Glue database:     ${GLUE_DB_NAME}
  • Glue crawler:      ${GLUE_CRAWLER_NAME}
  • Glue DQ rulesets:  ${DQ_RULESET_NAMES[*]}
  • IAM role (Lambda): ${LAMBDA_ROLE_NAME}
  • Lambda function:   ${LAMBDA_FUNCTION_NAME}
  • CW dashboard:      ${CW_DASHBOARD_NAME}
  • CW alarms:         ${CW_ALARM_NAMES[*]}

${BOLD}Region:${NC} ${AWS_REGION}
${BOLD}Profile:${NC} ${AWS_PROFILE}
${BOLD}Log file:${NC} ${LOG_FILE}
EOF
    exit 0
}

# ═══════════════════════════════════════════════════════════════════════════════
# PARSE ARGUMENTS
# ═══════════════════════════════════════════════════════════════════════════════
while [[ $# -gt 0 ]]; do
    case "$1" in
        --dry-run)  DRY_RUN=true; shift ;;
        --cleanup)  CLEANUP=true; shift ;;
        --update)   UPDATE=true; shift ;;
        --help|-h)  usage ;;
        *)          error "Unknown option: $1"; usage ;;
    esac
done

# ═══════════════════════════════════════════════════════════════════════════════
# PREREQUISITES CHECK
# ═══════════════════════════════════════════════════════════════════════════════
check_prerequisites() {
    header "Prerequisites Check"

    # AWS CLI
    if ! command -v aws &>/dev/null; then
        error "AWS CLI not found. Install: https://docs.aws.amazon.com/cli/latest/userguide/install-cliv2.html"
        exit 1
    fi
    local aws_version
    aws_version="$(aws --version 2>&1)"
    success "AWS CLI installed: ${aws_version}"

    # Python 3
    if ! command -v python3 &>/dev/null; then
        error "python3 not found. Required for synthetic data generation."
        exit 1
    fi
    success "Python3 installed: $(python3 --version 2>&1)"

    # jq (optional but helpful)
    if command -v jq &>/dev/null; then
        success "jq installed: $(jq --version 2>&1)"
    else
        warn "jq not found — JSON parsing will use aws --query instead"
    fi

    # zip (needed for Lambda packaging)
    if ! command -v zip &>/dev/null; then
        error "zip not found. Required for Lambda deployment packaging."
        exit 1
    fi
    success "zip installed"

    # AWS credentials
    info "Validating AWS credentials..."
    local sts_output
    if ! sts_output="$(aws sts get-caller-identity --region "${AWS_REGION}" --output json --no-cli-pager 2>&1)"; then
        error "AWS credentials not configured or expired."
        error "Run 'aws configure' or set AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY."
        error "STS output: ${sts_output}"
        exit 1
    fi
    AWS_ACCOUNT_ID="$(echo "${sts_output}" | python3 -c "import sys,json; print(json.load(sys.stdin)['Account'])")"
    local caller_arn
    caller_arn="$(echo "${sts_output}" | python3 -c "import sys,json; print(json.load(sys.stdin)['Arn'])")"
    success "AWS credentials valid — Account: ${AWS_ACCOUNT_ID}"
    info "Caller: ${caller_arn}"
    info "Region: ${AWS_REGION}"
    info "Profile: ${AWS_PROFILE}"
}

# ═══════════════════════════════════════════════════════════════════════════════
# SYNTHETIC DATA GENERATION
# ═══════════════════════════════════════════════════════════════════════════════
ensure_synth_data() {
    local data_dir="${DEMO_DIR}/synth_data/output"
    local csv_file="${data_dir}/patient_records.csv"
    local json_file="${data_dir}/patient_records.json"
    local notes_file="${data_dir}/clinical_notes.json"

    if [[ -f "${csv_file}" && -f "${json_file}" && -f "${notes_file}" ]]; then
        success "Synthetic data already exists at synth_data/output/"
        return 0
    fi

    info "Synthetic data not found — generating..."
    if ${DRY_RUN}; then
        dryrun "python3 ${DEMO_DIR}/synth_data/generate_healthcare_data.py"
        return 0
    fi

    (cd "${DEMO_DIR}" && python3 synth_data/generate_healthcare_data.py)
    success "Synthetic data generated"
}

ensure_glue_dq_outputs() {
    local all_exist=true
    local dqdl_dir="${DEMO_DIR}/glue_quality/output"
    for dqdl_file in "${DQ_RULESET_FILES[@]}"; do
        if ! [[ -f "${dqdl_dir}/${dqdl_file}" ]]; then
            all_exist=false
            break
        fi
    done
    if ${all_exist}; then
        success "Glue DQ outputs already exist (${#DQ_RULESET_FILES[@]} DQDL files)"
        return 0
    fi
    info "Glue DQ outputs not found — generating all ${#DQ_RULESET_FILES[@]} rulesets..."
    if ${DRY_RUN}; then
        dryrun "python3 ${DEMO_DIR}/glue_quality/glue_dq_rules.py"
        return 0
    fi
    (cd "${DEMO_DIR}" && python3 glue_quality/glue_dq_rules.py)
    success "Glue DQ outputs generated (${#DQ_RULESET_FILES[@]} DQDL files)"
}

ensure_cloudwatch_outputs() {
    local cw_dashboard="${DEMO_DIR}/cloudwatch/output/cloudwatch_dashboard.json"
    if [[ -f "${cw_dashboard}" ]]; then
        success "CloudWatch outputs already exist"
        return 0
    fi
    info "CloudWatch outputs not found — generating..."
    if ${DRY_RUN}; then
        dryrun "python3 ${DEMO_DIR}/cloudwatch/quality_dashboard.py"
        return 0
    fi
    (cd "${DEMO_DIR}" && python3 cloudwatch/quality_dashboard.py)
    success "CloudWatch outputs generated"
}

# ═══════════════════════════════════════════════════════════════════════════════
# STEP 1: S3 BUCKET
# ═══════════════════════════════════════════════════════════════════════════════
create_s3_bucket() {
    step "1" "S3 Bucket — ${BUCKET_NAME}"

    if ${DRY_RUN}; then
        dryrun "Create S3 bucket s3://${BUCKET_NAME} in ${AWS_REGION}"
        return 0
    fi

    if aws s3api head-bucket --bucket "${BUCKET_NAME}" --region "${AWS_REGION}" 2>/dev/null; then
        skip "Bucket ${BUCKET_NAME} already exists"
        ((SKIPPED++)) || true
    else
        info "Creating bucket ${BUCKET_NAME}..."
        # us-east-1 does not use LocationConstraint
        if [[ "${AWS_REGION}" == "us-east-1" ]]; then
            aws s3api create-bucket \
                --bucket "${BUCKET_NAME}" \
                --region "${AWS_REGION}" \
                --no-cli-pager
        else
            aws s3api create-bucket \
                --bucket "${BUCKET_NAME}" \
                --region "${AWS_REGION}" \
                --create-bucket-configuration "LocationConstraint=${AWS_REGION}" \
                --no-cli-pager
        fi

        # Block public access
        aws s3api put-public-access-block \
            --bucket "${BUCKET_NAME}" \
            --public-access-block-configuration \
            "BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true" \
            --region "${AWS_REGION}" \
            --no-cli-pager

        # Tag the bucket
        aws s3api put-bucket-tagging \
            --bucket "${BUCKET_NAME}" \
            --tagging "TagSet=[{Key=Project,Value=${TAG_PROJECT}},{Key=Demo,Value=${TAG_DEMO}}]" \
            --region "${AWS_REGION}" \
            --no-cli-pager

        success "Bucket ${BUCKET_NAME} created"
        ((CREATED++)) || true
    fi
}

# ═══════════════════════════════════════════════════════════════════════════════
# STEP 2: UPLOAD DATA
# ═══════════════════════════════════════════════════════════════════════════════
upload_data() {
    step "2" "Upload synthetic data to S3"

    local data_dir="${DEMO_DIR}/synth_data/output"

    if ${DRY_RUN}; then
        dryrun "Upload CSV as-is: patient_records.csv → s3://${BUCKET_NAME}/raw-data/"
        dryrun "Convert JSON arrays → JSON Lines and upload: patient_records.json, clinical_notes.json"
        return 0
    fi

    # Upload CSV directly (no conversion needed)
    info "Uploading patient_records.csv to S3..."
    aws s3 cp "${data_dir}/patient_records.csv" "s3://${BUCKET_NAME}/raw-data/patient_records.csv" \
        --region "${AWS_REGION}" --no-cli-pager
    success "Uploaded patient_records.csv"

    # Convert JSON array files → JSON Lines on-the-fly during upload.
    # WHY: Glue crawler treats a top-level JSON array as a single row with one "array" column.
    #       JSON Lines (one JSON object per line) produces proper multi-row tables with
    #       individual columns for each field. See AWS_AI_LEARNINGS.md entry #14.
    info "Converting patient_records.json → JSON Lines and uploading..."
    python3 -c "
import json, sys
with open(sys.argv[1]) as f:
    for item in json.load(f):
        print(json.dumps(item, ensure_ascii=False))
" "${data_dir}/patient_records.json" > "/tmp/patient_records.json"
    aws s3 cp "/tmp/patient_records.json" "s3://${BUCKET_NAME}/raw-data/patient_records.json" \
        --region "${AWS_REGION}" --no-cli-pager
    rm -f "/tmp/patient_records.json"
    success "Uploaded patient_records.json (JSON Lines)"

    info "Converting clinical_notes.json → JSON Lines and uploading..."
    python3 -c "
import json, sys
with open(sys.argv[1]) as f:
    for item in json.load(f):
        print(json.dumps(item, ensure_ascii=False))
" "${data_dir}/clinical_notes.json" > "/tmp/clinical_notes.json"
    aws s3 cp "/tmp/clinical_notes.json" "s3://${BUCKET_NAME}/raw-data/clinical_notes.json" \
        --region "${AWS_REGION}" --no-cli-pager
    rm -f "/tmp/clinical_notes.json"
    success "Uploaded clinical_notes.json (JSON Lines)"

    ((CREATED++)) || true
}

# ═══════════════════════════════════════════════════════════════════════════════
# STEP 3: IAM ROLE FOR GLUE
# ═══════════════════════════════════════════════════════════════════════════════
create_glue_iam_role() {
    step "3" "IAM Role for Glue — ${GLUE_CRAWLER_ROLE_NAME}"

    if ${DRY_RUN}; then
        dryrun "Create IAM role ${GLUE_CRAWLER_ROLE_NAME} with glue.amazonaws.com trust"
        dryrun "Attach AWSGlueServiceRole managed policy"
        dryrun "Attach inline policy for S3 read access to ${BUCKET_NAME}"
        return 0
    fi

    # Check if role exists
    if aws iam get-role --role-name "${GLUE_CRAWLER_ROLE_NAME}" --no-cli-pager 2>/dev/null; then
        skip "IAM role ${GLUE_CRAWLER_ROLE_NAME} already exists"
        # Always update the inline policy to ensure lakeformation + S3 write perms are current
        info "Updating inline policy to ensure Lake Formation + S3 write permissions..."
        local s3_policy_update
        s3_policy_update=$(cat <<S3U_EOF
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "S3ReadAccess",
            "Effect": "Allow",
            "Action": [
                "s3:GetObject",
                "s3:ListBucket"
            ],
            "Resource": [
                "arn:aws:s3:::${BUCKET_NAME}",
                "arn:aws:s3:::${BUCKET_NAME}/*"
            ]
        },
        {
            "Sid": "S3WriteForDQResults",
            "Effect": "Allow",
            "Action": [
                "s3:PutObject"
            ],
            "Resource": [
                "arn:aws:s3:::${BUCKET_NAME}/dq-results/*"
            ]
        },
        {
            "Sid": "LakeFormationCredentialVending",
            "Effect": "Allow",
            "Action": [
                "lakeformation:GetDataAccess"
            ],
            "Resource": "*"
        }
    ]
}
S3U_EOF
        )
        aws iam put-role-policy \
            --role-name "${GLUE_CRAWLER_ROLE_NAME}" \
            --policy-name "S3ReadAccess-${BUCKET_NAME}" \
            --policy-document "${s3_policy_update}" \
            --no-cli-pager
        success "Inline policy updated with Lake Formation + S3 write permissions"
        ((SKIPPED++)) || true
    else
        info "Creating IAM role ${GLUE_CRAWLER_ROLE_NAME}..."

        # Trust policy for Glue
        local trust_policy
        trust_policy=$(cat <<'TRUST_EOF'
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Principal": {
                "Service": "glue.amazonaws.com"
            },
            "Action": "sts:AssumeRole"
        }
    ]
}
TRUST_EOF
        )

        aws iam create-role \
            --role-name "${GLUE_CRAWLER_ROLE_NAME}" \
            --assume-role-policy-document "${trust_policy}" \
            --description "Glue service role for healthcare records crawler and DQ" \
            --tags "Key=Project,Value=${TAG_PROJECT}" "Key=Demo,Value=${TAG_DEMO}" \
            --no-cli-pager

        # Attach AWS managed policy for Glue
        aws iam attach-role-policy \
            --role-name "${GLUE_CRAWLER_ROLE_NAME}" \
            --policy-arn "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole" \
            --no-cli-pager

        # Inline policy for S3 bucket read/write access + Lake Formation credential vending
        local s3_policy
        s3_policy=$(cat <<S3_EOF
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "S3ReadAccess",
            "Effect": "Allow",
            "Action": [
                "s3:GetObject",
                "s3:ListBucket"
            ],
            "Resource": [
                "arn:aws:s3:::${BUCKET_NAME}",
                "arn:aws:s3:::${BUCKET_NAME}/*"
            ]
        },
        {
            "Sid": "S3WriteForDQResults",
            "Effect": "Allow",
            "Action": [
                "s3:PutObject"
            ],
            "Resource": [
                "arn:aws:s3:::${BUCKET_NAME}/dq-results/*"
            ]
        },
        {
            "Sid": "LakeFormationCredentialVending",
            "Effect": "Allow",
            "Action": [
                "lakeformation:GetDataAccess"
            ],
            "Resource": "*"
        }
    ]
}
S3_EOF
        )

        aws iam put-role-policy \
            --role-name "${GLUE_CRAWLER_ROLE_NAME}" \
            --policy-name "S3ReadAccess-${BUCKET_NAME}" \
            --policy-document "${s3_policy}" \
            --no-cli-pager

        success "IAM role ${GLUE_CRAWLER_ROLE_NAME} created with AWSGlueServiceRole + S3 read + Lake Formation"
        ((CREATED++)) || true

        # Wait for IAM propagation
        info "Waiting 10s for IAM role propagation..."
        sleep 10
    fi
}

# ═══════════════════════════════════════════════════════════════════════════════
# STEP 4: GLUE DATABASE
# ═══════════════════════════════════════════════════════════════════════════════
create_glue_database() {
    step "4" "Glue Database — ${GLUE_DB_NAME}"

    if ${DRY_RUN}; then
        dryrun "Create Glue database ${GLUE_DB_NAME}"
        return 0
    fi

    if aws glue get-database --name "${GLUE_DB_NAME}" --region "${AWS_REGION}" --no-cli-pager 2>/dev/null; then
        skip "Glue database ${GLUE_DB_NAME} already exists"
        ((SKIPPED++)) || true
    else
        info "Creating Glue database ${GLUE_DB_NAME}..."
        aws glue create-database \
            --region "${AWS_REGION}" \
            --database-input "{
                \"Name\": \"${GLUE_DB_NAME}\",
                \"Description\": \"Healthcare patient records for Demo 1 — AIP-C01\"
            }" \
            --no-cli-pager
        success "Glue database ${GLUE_DB_NAME} created"
        ((CREATED++)) || true
    fi
}

# ═══════════════════════════════════════════════════════════════════════════════
# STEP 4b: LAKE FORMATION PERMISSIONS
# ═══════════════════════════════════════════════════════════════════════════════
grant_lakeformation_permissions() {
    step "4b" "Lake Formation Permissions for Glue Crawler Role"

    local role_arn="arn:aws:iam::${AWS_ACCOUNT_ID}:role/${GLUE_CRAWLER_ROLE_NAME}"

    if ${DRY_RUN}; then
        dryrun "Grant Lake Formation permissions (CREATE_TABLE, DESCRIBE, ALTER, DROP) on database ${GLUE_DB_NAME} to ${GLUE_CRAWLER_ROLE_NAME}"
        dryrun "Grant Lake Formation TABLE wildcard permissions on ${GLUE_DB_NAME} to Glue role and caller"
        return 0
    fi

    # ── Part A: DATABASE-level permissions for Glue crawler role ──
    info "Checking existing Lake Formation DATABASE permissions for ${GLUE_CRAWLER_ROLE_NAME} on ${GLUE_DB_NAME}..."
    local existing
    existing=$(aws lakeformation list-permissions \
        --principal "DataLakePrincipalIdentifier=${role_arn}" \
        --resource-type "DATABASE" \
        --resource '{"Database": {"Name": "'"${GLUE_DB_NAME}"'"}}' \
        --region "${AWS_REGION}" \
        --query "PrincipalResourcePermissions" \
        --output json \
        --no-cli-pager 2>/dev/null || echo "[]")

    if [[ -n "${existing}" && "${existing}" != "[]" ]]; then
        skip "Lake Formation DATABASE permissions already granted on ${GLUE_DB_NAME} for ${GLUE_CRAWLER_ROLE_NAME}"
        ((SKIPPED++)) || true
    else
        info "Granting Lake Formation DATABASE permissions on ${GLUE_DB_NAME}..."
        aws lakeformation grant-permissions \
            --principal "DataLakePrincipalIdentifier=${role_arn}" \
            --permissions "CREATE_TABLE" "DESCRIBE" "ALTER" "DROP" \
            --resource '{"Database": {"Name": "'"${GLUE_DB_NAME}"'"}}' \
            --region "${AWS_REGION}" \
            --no-cli-pager
        success "Lake Formation DATABASE permissions granted: CREATE_TABLE, DESCRIBE, ALTER, DROP on ${GLUE_DB_NAME}"
        ((CREATED++)) || true
    fi

    # ── Part B: TABLE wildcard permissions ──
    # CRITICAL: When Lake Formation's CreateTableDefaultPermissions is empty (modern default),
    # tables created by the crawler are INVISIBLE to all principals except the LF admin.
    # We must grant TABLE-level permissions using a wildcard (covers all current + future tables).
    # See AWS_AI_LEARNINGS.md entry #11 for the full explanation.

    # Determine the caller's IAM role ARN (for granting table visibility to the deployer)
    local caller_arn
    caller_arn="$(aws sts get-caller-identity --region "${AWS_REGION}" --query "Arn" --output text --no-cli-pager)"
    # Extract the role ARN from assumed-role ARN: arn:aws:sts::ACCT:assumed-role/ROLENAME/SESSION → arn:aws:iam::ACCT:role/ROLENAME
    local caller_role_arn=""
    if [[ "${caller_arn}" == *":assumed-role/"* ]]; then
        local caller_role_name
        caller_role_name="$(echo "${caller_arn}" | sed 's|.*:assumed-role/\([^/]*\)/.*|\1|')"
        caller_role_arn="arn:aws:iam::${AWS_ACCOUNT_ID}:role/${caller_role_name}"
    elif [[ "${caller_arn}" == *":role/"* ]]; then
        caller_role_arn="${caller_arn}"
    elif [[ "${caller_arn}" == *":user/"* ]]; then
        caller_role_arn="${caller_arn}"
    fi

    # Grant TABLE wildcard to the Glue crawler role
    local table_existing
    table_existing=$(aws lakeformation list-permissions \
        --principal "DataLakePrincipalIdentifier=${role_arn}" \
        --resource-type "TABLE" \
        --resource '{"Table": {"DatabaseName": "'"${GLUE_DB_NAME}"'", "TableWildcard": {}}}' \
        --region "${AWS_REGION}" \
        --query "PrincipalResourcePermissions" \
        --output json \
        --no-cli-pager 2>/dev/null || echo "[]")

    if [[ -n "${table_existing}" && "${table_existing}" != "[]" ]]; then
        skip "Lake Formation TABLE wildcard permissions already granted for ${GLUE_CRAWLER_ROLE_NAME}"
    else
        info "Granting Lake Formation TABLE wildcard permissions to Glue crawler role..."
        aws lakeformation grant-permissions \
            --principal "DataLakePrincipalIdentifier=${role_arn}" \
            --permissions "ALL" "SELECT" "ALTER" "DROP" "DELETE" "INSERT" "DESCRIBE" \
            --permissions-with-grant-option "ALL" "SELECT" "ALTER" "DROP" "DELETE" "INSERT" "DESCRIBE" \
            --resource '{"Table": {"DatabaseName": "'"${GLUE_DB_NAME}"'", "TableWildcard": {}}}' \
            --region "${AWS_REGION}" \
            --no-cli-pager
        success "Lake Formation TABLE wildcard permissions granted to Glue crawler role"
        ((CREATED++)) || true
    fi

    # Grant TABLE wildcard to the caller's role (so get-tables works during script execution)
    if [[ -n "${caller_role_arn}" && "${caller_role_arn}" != "${role_arn}" ]]; then
        local caller_table_existing
        caller_table_existing=$(aws lakeformation list-permissions \
            --principal "DataLakePrincipalIdentifier=${caller_role_arn}" \
            --resource-type "TABLE" \
            --resource '{"Table": {"DatabaseName": "'"${GLUE_DB_NAME}"'", "TableWildcard": {}}}' \
            --region "${AWS_REGION}" \
            --query "PrincipalResourcePermissions" \
            --output json \
            --no-cli-pager 2>/dev/null || echo "[]")

        if [[ -n "${caller_table_existing}" && "${caller_table_existing}" != "[]" ]]; then
            skip "Lake Formation TABLE wildcard permissions already granted for caller role"
        else
            info "Granting Lake Formation TABLE wildcard permissions to caller role (${caller_role_arn})..."
            aws lakeformation grant-permissions \
                --principal "DataLakePrincipalIdentifier=${caller_role_arn}" \
                --permissions "ALL" "SELECT" "ALTER" "DROP" "DELETE" "INSERT" "DESCRIBE" \
                --permissions-with-grant-option "ALL" "SELECT" "ALTER" "DROP" "DELETE" "INSERT" "DESCRIBE" \
                --resource '{"Table": {"DatabaseName": "'"${GLUE_DB_NAME}"'", "TableWildcard": {}}}' \
                --region "${AWS_REGION}" \
                --no-cli-pager
            success "Lake Formation TABLE wildcard permissions granted to caller role"
            ((CREATED++)) || true
        fi
    fi
}

# ═══════════════════════════════════════════════════════════════════════════════
# STEP 5: GLUE CRAWLER
# ═══════════════════════════════════════════════════════════════════════════════
create_glue_crawler() {
    step "5" "Glue Crawler — ${GLUE_CRAWLER_NAME}"

    local role_arn="arn:aws:iam::${AWS_ACCOUNT_ID}:role/${GLUE_CRAWLER_ROLE_NAME}"
    local s3_target="s3://${BUCKET_NAME}/raw-data/"

    if ${DRY_RUN}; then
        dryrun "Create Glue crawler ${GLUE_CRAWLER_NAME} targeting ${s3_target}"
        dryrun "Start crawler and wait for completion"
        return 0
    fi

    if aws glue get-crawler --name "${GLUE_CRAWLER_NAME}" --region "${AWS_REGION}" --no-cli-pager 2>/dev/null; then
        skip "Glue crawler ${GLUE_CRAWLER_NAME} already exists"
        ((SKIPPED++)) || true
    else
        info "Creating Glue crawler ${GLUE_CRAWLER_NAME}..."

        local targets_json
        targets_json=$(cat <<TARGETS_EOF
{
    "S3Targets": [
        {
            "Path": "${s3_target}"
        }
    ]
}
TARGETS_EOF
        )

        aws glue create-crawler \
            --name "${GLUE_CRAWLER_NAME}" \
            --role "${role_arn}" \
            --database-name "${GLUE_DB_NAME}" \
            --targets "${targets_json}" \
            --description "Crawls patient_records.csv from S3 for healthcare demo" \
            --tags "Project=${TAG_PROJECT},Demo=${TAG_DEMO}" \
            --region "${AWS_REGION}" \
            --no-cli-pager

        success "Glue crawler ${GLUE_CRAWLER_NAME} created"
        ((CREATED++)) || true
    fi

    # Start the crawler
    info "Starting crawler ${GLUE_CRAWLER_NAME}..."
    local crawler_state
    crawler_state="$(aws glue get-crawler \
        --name "${GLUE_CRAWLER_NAME}" \
        --region "${AWS_REGION}" \
        --query "Crawler.State" \
        --output text \
        --no-cli-pager)"

    if [[ "${crawler_state}" == "RUNNING" ]]; then
        info "Crawler is already running — waiting for completion..."
    elif [[ "${crawler_state}" == "READY" ]]; then
        aws glue start-crawler \
            --name "${GLUE_CRAWLER_NAME}" \
            --region "${AWS_REGION}" \
            --no-cli-pager
        info "Crawler started"
    else
        warn "Crawler state is ${crawler_state} — attempting to start..."
        aws glue start-crawler \
            --name "${GLUE_CRAWLER_NAME}" \
            --region "${AWS_REGION}" \
            --no-cli-pager || warn "Could not start crawler (state: ${crawler_state})"
    fi

    # Wait for crawler to finish (max ~5 minutes)
    info "Waiting for crawler to complete (timeout: 300s)..."
    local elapsed=0
    local max_wait=300
    local crawler_succeeded=false
    while [[ ${elapsed} -lt ${max_wait} ]]; do
        crawler_state="$(aws glue get-crawler \
            --name "${GLUE_CRAWLER_NAME}" \
            --region "${AWS_REGION}" \
            --query "Crawler.State" \
            --output text \
            --no-cli-pager)"
        if [[ "${crawler_state}" == "READY" ]]; then
            success "Crawler completed successfully"
            crawler_succeeded=true
            break
        fi
        sleep 10
        elapsed=$((elapsed + 10))
        info "  Crawler state: ${crawler_state} (${elapsed}s elapsed)..."
    done

    if ! ${crawler_succeeded}; then
        warn "Crawler did not complete within ${max_wait}s — check AWS console"
    fi

    # ── Discover table name(s) created by the crawler ──
    info "Discovering tables created by crawler in database ${GLUE_DB_NAME}..."
    local discovered_tables
    discovered_tables="$(aws glue get-tables \
        --database-name "${GLUE_DB_NAME}" \
        --region "${AWS_REGION}" \
        --query "TableList[].Name" \
        --output text \
        --no-cli-pager 2>/dev/null || echo "")"

    if [[ -n "${discovered_tables}" && "${discovered_tables}" != "None" ]]; then
        # Use the first table discovered
        DISCOVERED_TABLE_NAME="$(echo "${discovered_tables}" | awk '{print $1}')"
        success "Discovered table: ${DISCOVERED_TABLE_NAME} (all tables: ${discovered_tables})"
        if [[ "${DISCOVERED_TABLE_NAME}" != "${GLUE_TABLE_NAME}" ]]; then
            warn "Discovered table name '${DISCOVERED_TABLE_NAME}' differs from default '${GLUE_TABLE_NAME}' — will use discovered name"
        fi
    else
        warn "No tables found in database ${GLUE_DB_NAME} — crawler may not have completed properly"
        warn "Subsequent steps will attempt to use fallback table name '${GLUE_TABLE_NAME}'"
    fi
}

# ═══════════════════════════════════════════════════════════════════════════════
# STEP 6: GLUE DATA QUALITY RULESETS (ALL TABLES)
# ═══════════════════════════════════════════════════════════════════════════════
create_glue_dq_rulesets() {
    step "6" "Glue Data Quality Rulesets — ${#DQ_TABLE_NAMES[@]} tables"

    if ${DRY_RUN}; then
        for i in "${!DQ_TABLE_NAMES[@]}"; do
            dryrun "Create DQ ruleset ${DQ_RULESET_NAMES[$i]} for table ${DQ_TABLE_NAMES[$i]}"
        done
        return 0
    fi

    # Clean up legacy ruleset name if it exists (from pre-multi-table era)
    if [[ -n "${GLUE_DQ_RULESET_NAME_LEGACY}" ]] && \
       aws glue get-data-quality-ruleset --name "${GLUE_DQ_RULESET_NAME_LEGACY}" --region "${AWS_REGION}" --no-cli-pager 2>/dev/null; then
        info "Deleting legacy ruleset: ${GLUE_DQ_RULESET_NAME_LEGACY} (replaced by per-table rulesets)"
        aws glue delete-data-quality-ruleset --name "${GLUE_DQ_RULESET_NAME_LEGACY}" --region "${AWS_REGION}" --no-cli-pager 2>/dev/null || true
        success "Legacy ruleset deleted"
    fi

    # Discover all tables in the database
    local discovered_tables
    discovered_tables="$(aws glue get-tables \
        --database-name "${GLUE_DB_NAME}" \
        --region "${AWS_REGION}" \
        --query "TableList[].Name" \
        --output text \
        --no-cli-pager 2>/dev/null || echo "")"
    info "Tables in ${GLUE_DB_NAME}: ${discovered_tables:-none}"

    # Create a ruleset for each table
    for i in "${!DQ_TABLE_NAMES[@]}"; do
        local table_name="${DQ_TABLE_NAMES[$i]}"
        local ruleset_name="${DQ_RULESET_NAMES[$i]}"
        local ruleset_file="${DEMO_DIR}/glue_quality/output/${DQ_RULESET_FILES[$i]}"
        local ruleset_desc="${DQ_RULESET_DESCS[$i]}"

        info "── Ruleset ${i}: ${ruleset_name} → ${table_name} ──"

        if ! [[ -f "${ruleset_file}" ]]; then
            error "DQDL file not found: ${ruleset_file}"
            ((ERRORS++)) || true
            continue
        fi

        # Check if ruleset already exists
        if aws glue get-data-quality-ruleset --name "${ruleset_name}" --region "${AWS_REGION}" --no-cli-pager 2>/dev/null; then
            skip "Ruleset ${ruleset_name} already exists"
            ((SKIPPED++)) || true
            continue
        fi

        # Check if table exists in the catalog
        local table_exists=false
        if echo "${discovered_tables}" | grep -qw "${table_name}"; then
            table_exists=true
        fi

        local dqdl_content
        dqdl_content="$(cat "${ruleset_file}")"

        if ${table_exists}; then
            info "Binding ruleset to table: ${GLUE_DB_NAME}.${table_name}"
            aws glue create-data-quality-ruleset \
                --name "${ruleset_name}" \
                --description "${ruleset_desc}" \
                --ruleset "${dqdl_content}" \
                --target-table "{\"TableName\": \"${table_name}\", \"DatabaseName\": \"${GLUE_DB_NAME}\"}" \
                --region "${AWS_REGION}" \
                --no-cli-pager
        else
            warn "Table ${table_name} not found in catalog — creating ruleset without binding"
            warn "(Bind at evaluation time via --data-source)"
            aws glue create-data-quality-ruleset \
                --name "${ruleset_name}" \
                --description "${ruleset_desc}" \
                --ruleset "${dqdl_content}" \
                --region "${AWS_REGION}" \
                --no-cli-pager
        fi

        success "Ruleset created: ${ruleset_name}"
        ((CREATED++)) || true
    done
}

# ═══════════════════════════════════════════════════════════════════════════════
# STEP 6b: GLUE DATA QUALITY — EVALUATION RUNS (ALL TABLES)
# ═══════════════════════════════════════════════════════════════════════════════
run_dq_evaluations() {
    step "6b" "Glue Data Quality — Evaluation Runs (${#DQ_TABLE_NAMES[@]} tables)"

    local role_arn="arn:aws:iam::${AWS_ACCOUNT_ID}:role/${GLUE_CRAWLER_ROLE_NAME}"

    if ${DRY_RUN}; then
        for i in "${!DQ_TABLE_NAMES[@]}"; do
            dryrun "Evaluate ${DQ_RULESET_NAMES[$i]} against ${GLUE_DB_NAME}.${DQ_TABLE_NAMES[$i]}"
        done
        return 0
    fi

    for i in "${!DQ_TABLE_NAMES[@]}"; do
        local table_name="${DQ_TABLE_NAMES[$i]}"
        local ruleset_name="${DQ_RULESET_NAMES[$i]}"

        echo ""
        info "━━━ Evaluation ${i}: ${ruleset_name} → ${table_name} ━━━"

        # Verify table exists
        if ! aws glue get-table --database-name "${GLUE_DB_NAME}" --name "${table_name}" \
            --region "${AWS_REGION}" --no-cli-pager 2>/dev/null; then
            warn "Table ${table_name} not found — skipping evaluation"
            continue
        fi

        # Verify ruleset exists
        if ! aws glue get-data-quality-ruleset --name "${ruleset_name}" \
            --region "${AWS_REGION}" --no-cli-pager 2>/dev/null; then
            warn "Ruleset ${ruleset_name} not found — skipping evaluation"
            continue
        fi

        # Check for existing recent result
        local recent_result
        recent_result="$(aws glue list-data-quality-results \
            --filter "{\"DataSource\": {\"GlueTable\": {\"DatabaseName\": \"${GLUE_DB_NAME}\", \"TableName\": \"${table_name}\"}}}" \
            --region "${AWS_REGION}" \
            --query "Results[0].ResultId" \
            --output text \
            --no-cli-pager 2>/dev/null || echo "None")"

        if [[ -n "${recent_result}" && "${recent_result}" != "None" ]]; then
            info "Found existing DQ result for ${table_name}: ${recent_result}"
            display_dq_result "${recent_result}" "${table_name}"
            ((SKIPPED++)) || true
            continue
        fi

        # Start evaluation run
        info "Starting DQ evaluation: ${ruleset_name} → ${GLUE_DB_NAME}.${table_name}"
        local run_output
        run_output="$(aws glue start-data-quality-ruleset-evaluation-run \
            --data-source "{\"GlueTable\": {\"DatabaseName\": \"${GLUE_DB_NAME}\", \"TableName\": \"${table_name}\"}}" \
            --role "${role_arn}" \
            --ruleset-names "${ruleset_name}" \
            --number-of-workers 2 \
            --timeout 10 \
            --additional-run-options "{\"CloudWatchMetricsEnabled\": true, \"ResultsS3Prefix\": \"s3://${BUCKET_NAME}/dq-results/${table_name}/\"}" \
            --region "${AWS_REGION}" \
            --output json \
            --no-cli-pager 2>&1)" || {
            error "Failed to start DQ evaluation for ${table_name}: ${run_output}"
            ((ERRORS++)) || true
            continue
        }

        local run_id
        run_id="$(echo "${run_output}" | python3 -c "import sys,json; print(json.load(sys.stdin)['RunId'])" 2>/dev/null || echo "")"

        if [[ -z "${run_id}" ]]; then
            error "Could not extract RunId for ${table_name}: ${run_output}"
            ((ERRORS++)) || true
            continue
        fi

        success "Evaluation started for ${table_name}: RunId=${run_id}"
        progress "IN_PROGRESS" "DQ evaluation started for ${table_name}: ${run_id}"

        # Poll for completion (max 600s)
        info "Waiting for evaluation to complete (timeout: 600s)..."
        local elapsed=0
        local max_wait=600
        local run_status=""
        local eval_succeeded=false

        while [[ ${elapsed} -lt ${max_wait} ]]; do
            local run_details
            run_details="$(aws glue get-data-quality-ruleset-evaluation-run \
                --run-id "${run_id}" \
                --region "${AWS_REGION}" \
                --output json \
                --no-cli-pager 2>/dev/null || echo "{}")"

            run_status="$(echo "${run_details}" | python3 -c "import sys,json; print(json.load(sys.stdin).get('Status','UNKNOWN'))" 2>/dev/null || echo "UNKNOWN")"

            case "${run_status}" in
                SUCCEEDED)
                    eval_succeeded=true
                    break
                    ;;
                FAILED|TIMEOUT|ERROR|STOPPED)
                    local error_str
                    error_str="$(echo "${run_details}" | python3 -c "import sys,json; print(json.load(sys.stdin).get('ErrorString','No error details'))" 2>/dev/null || echo "No error details")"
                    error "DQ evaluation for ${table_name} ${run_status}: ${error_str}"
                    ((ERRORS++)) || true
                    break
                    ;;
                *)
                    sleep 15
                    elapsed=$((elapsed + 15))
                    info "  ${table_name}: ${run_status} (${elapsed}s elapsed)..."
                    ;;
            esac
        done

        if ! ${eval_succeeded}; then
            [[ ${elapsed} -ge ${max_wait} ]] && warn "Evaluation for ${table_name} timed out (${max_wait}s)"
            continue
        fi

        # Retrieve result IDs
        local result_ids
        result_ids="$(aws glue get-data-quality-ruleset-evaluation-run \
            --run-id "${run_id}" \
            --region "${AWS_REGION}" \
            --query "ResultIds[]" \
            --output text \
            --no-cli-pager 2>/dev/null || echo "")"

        for result_id in ${result_ids}; do
            display_dq_result "${result_id}" "${table_name}"
        done

        success "Evaluation completed for ${table_name}"
        progress "COMPLETED" "DQ evaluation completed for ${table_name}: ${run_id}"
        ((CREATED++)) || true
    done
}

# Helper: Display a DQ result with formatting
display_dq_result() {
    local result_id="$1"
    local display_table="${2:-${DISCOVERED_TABLE_NAME:-${GLUE_TABLE_NAME}}}"

    info "Retrieving DQ result: ${result_id}..."

    local result_json
    result_json="$(aws glue get-data-quality-result \
        --result-id "${result_id}" \
        --region "${AWS_REGION}" \
        --output json \
        --no-cli-pager 2>/dev/null || echo "{}")"

    if [[ "${result_json}" == "{}" ]]; then
        warn "Could not retrieve result ${result_id}"
        return
    fi

    # Extract the overall score
    local score
    score="$(echo "${result_json}" | python3 -c "import sys,json; d=json.load(sys.stdin); print(f\"{d.get('Score',0)*100:.1f}%\")" 2>/dev/null || echo "N/A")"

    echo ""
    echo -e "${BOLD}${CYAN}  ┌─────────────────────────────────────────────────────────┐${NC}"
    echo -e "${BOLD}${CYAN}  │  📊 Glue Data Quality Score: ${GREEN}${score}${CYAN}                        │${NC}"
    echo -e "${BOLD}${CYAN}  └─────────────────────────────────────────────────────────┘${NC}"
    echo ""

    # Display per-rule results using a temp file for safe JSON handling
    local tmp_result="/tmp/dq_result_$$.json"
    echo "${result_json}" > "${tmp_result}"
    python3 -c "
import json, sys

with open('${tmp_result}') as f:
    result = json.load(f)

# Per-rule results
rules = result.get('RuleResults', [])
passed = sum(1 for r in rules if r.get('Result') == 'PASS')
failed = sum(1 for r in rules if r.get('Result') == 'FAIL')
errored = sum(1 for r in rules if r.get('Result') == 'ERROR')
total = len(rules)
print(f'  Rules: {passed} passed, {failed} failed, {errored} errored (of {total} total)')
print()

if rules:
    print('  Rule-by-Rule Results:')
    print('  ' + chr(9472) * 75)
    for r in rules:
        name = r.get('Name', 'Unknown')
        desc = r.get('Description', '')
        status = r.get('Result', 'UNKNOWN')
        msg = r.get('EvaluationMessage', '')
        metrics = r.get('EvaluatedMetrics', {})
        icon = chr(0x2705) if status == 'PASS' else chr(0x274C) if status == 'FAIL' else chr(0x26A0)
        # Truncate description for display
        short_desc = desc[:65] + '...' if len(desc) > 65 else desc
        print(f'  {icon} {status:5s} | {short_desc}')
        if msg and status != 'PASS':
            for line in msg.split(chr(10))[:2]:
                print(f'           |   {line[:85]}')
        if metrics and status != 'PASS':
            metric_str = ', '.join(f'{k}={v}' for k, v in metrics.items())
            print(f'           |   Metrics: {metric_str[:85]}')
    print('  ' + chr(9472) * 75)

# Observations (anomaly detection insights)
obs = result.get('Observations', [])
if obs:
    print()
    print('  ' + chr(0x1F4C8) + ' Observations (anomaly detection insights):')
    for o in obs:
        desc = o.get('Description', '')
        metric_obs = o.get('MetricBasedObservation', {})
        if metric_obs:
            metric_name = metric_obs.get('MetricName', '')
            values = metric_obs.get('MetricValues', {})
            actual = values.get('ActualValue', 'N/A')
            expected = values.get('ExpectedValue', 'N/A')
            lower = values.get('LowerLimit', 'N/A')
            upper = values.get('UpperLimit', 'N/A')
            print(f'  * {metric_name}: actual={actual}, expected={expected} (range: {lower} - {upper})')
        elif desc:
            print(f'  * {desc}')

# Analyzer results
analyzers = result.get('AnalyzerResults', [])
if analyzers:
    print()
    print('  ' + chr(0x1F50D) + ' Analyzer Statistics (for anomaly detection baseline):')
    for a in analyzers:
        name = a.get('Name', 'Unknown')
        desc = a.get('Description', '')
        metrics = a.get('EvaluatedMetrics', {})
        metric_str = ', '.join(f'{k}={v:.4f}' if isinstance(v, float) else f'{k}={v}' for k, v in metrics.items())
        print(f'  * {name}: {metric_str}')
" 2>/dev/null || warn "Could not parse DQ result details"
    rm -f "${tmp_result}"

    echo ""
    info "Result ID: ${result_id}"
    info "View in console: https://${AWS_REGION}.console.aws.amazon.com/glue/home?region=${AWS_REGION}#/v2/data-catalog/table/view/${display_table}?database=${GLUE_DB_NAME}&tab=dataQuality"
}

# ═══════════════════════════════════════════════════════════════════════════════
# STEP 6c: GLUE DATA QUALITY — RULE RECOMMENDATION (Optional)
# ═══════════════════════════════════════════════════════════════════════════════
run_dq_recommendation() {
    step "6c" "Glue Data Quality — Rule Recommendation (Optional)"

    local role_arn="arn:aws:iam::${AWS_ACCOUNT_ID}:role/${GLUE_CRAWLER_ROLE_NAME}"
    local effective_table_name="${DISCOVERED_TABLE_NAME:-${GLUE_TABLE_NAME}}"

    if ${DRY_RUN}; then
        dryrun "Start DQ rule recommendation run on ${GLUE_DB_NAME}.${effective_table_name}"
        dryrun "Wait for recommendation to complete (up to 600s)"
        dryrun "Display Glue's auto-recommended rules"
        return 0
    fi

    if [[ -z "${effective_table_name}" || "${effective_table_name}" == "None" ]]; then
        warn "No table name available — skipping DQ recommendation run"
        return 0
    fi

    # Check if there are already existing recommendation runs
    local existing_recs
    existing_recs="$(aws glue list-data-quality-rule-recommendation-runs \
        --filter "{\"DataSource\": {\"GlueTable\": {\"DatabaseName\": \"${GLUE_DB_NAME}\", \"TableName\": \"${effective_table_name}\"}}}" \
        --region "${AWS_REGION}" \
        --query "Runs[0].RunId" \
        --output text \
        --no-cli-pager 2>/dev/null || echo "None")"

    if [[ -n "${existing_recs}" && "${existing_recs}" != "None" ]]; then
        info "Found existing recommendation run: ${existing_recs}"
        # Retrieve the recommended ruleset if available
        local rec_details
        rec_details="$(aws glue get-data-quality-rule-recommendation-run \
            --run-id "${existing_recs}" \
            --region "${AWS_REGION}" \
            --output json \
            --no-cli-pager 2>/dev/null || echo "{}")"
        local rec_status
        rec_status="$(echo "${rec_details}" | python3 -c "import sys,json; print(json.load(sys.stdin).get('Status','UNKNOWN'))" 2>/dev/null || echo "UNKNOWN")"
        if [[ "${rec_status}" == "SUCCEEDED" ]]; then
            local rec_ruleset
            rec_ruleset="$(echo "${rec_details}" | python3 -c "import sys,json; print(json.load(sys.stdin).get('RecommendedRuleset','N/A'))" 2>/dev/null || echo "N/A")"
            echo ""
            echo -e "${BOLD}${CYAN}  ┌─────────────────────────────────────────────────────────┐${NC}"
            echo -e "${BOLD}${CYAN}  │  🤖 Glue's Auto-Recommended Rules (cached)              │${NC}"
            echo -e "${BOLD}${CYAN}  └─────────────────────────────────────────────────────────┘${NC}"
            echo ""
            echo "${rec_ruleset}" | head -40
            echo ""
            skip "Using cached recommendation run ${existing_recs}"
            ((SKIPPED++)) || true
            return 0
        fi
    fi

    info "Starting DQ rule recommendation run..."
    info "  Table:  ${GLUE_DB_NAME}.${effective_table_name}"
    info "  Role:   ${GLUE_CRAWLER_ROLE_NAME}"
    info "  (Glue will analyze the data and suggest rules automatically)"

    local rec_output
    rec_output="$(aws glue start-data-quality-rule-recommendation-run \
        --data-source "{\"GlueTable\": {\"DatabaseName\": \"${GLUE_DB_NAME}\", \"TableName\": \"${effective_table_name}\"}}" \
        --role "${role_arn}" \
        --number-of-workers 2 \
        --timeout 10 \
        --region "${AWS_REGION}" \
        --output json \
        --no-cli-pager 2>&1)" || {
        warn "Could not start recommendation run: ${rec_output}"
        warn "This is optional — proceeding without recommendations"
        return 0
    }

    local rec_run_id
    rec_run_id="$(echo "${rec_output}" | python3 -c "import sys,json; print(json.load(sys.stdin)['RunId'])" 2>/dev/null || echo "")"

    if [[ -z "${rec_run_id}" ]]; then
        warn "Could not extract RunId — skipping recommendation"
        return 0
    fi

    success "Recommendation run started: RunId=${rec_run_id}"

    # Poll for completion (max 600s)
    info "Waiting for recommendation to complete (timeout: 600s)..."
    local elapsed=0
    local max_wait=600
    local rec_status=""

    while [[ ${elapsed} -lt ${max_wait} ]]; do
        local rec_details
        rec_details="$(aws glue get-data-quality-rule-recommendation-run \
            --run-id "${rec_run_id}" \
            --region "${AWS_REGION}" \
            --output json \
            --no-cli-pager 2>/dev/null || echo "{}")"

        rec_status="$(echo "${rec_details}" | python3 -c "import sys,json; print(json.load(sys.stdin).get('Status','UNKNOWN'))" 2>/dev/null || echo "UNKNOWN")"

        case "${rec_status}" in
            SUCCEEDED)
                local rec_ruleset
                rec_ruleset="$(echo "${rec_details}" | python3 -c "import sys,json; print(json.load(sys.stdin).get('RecommendedRuleset','N/A'))" 2>/dev/null || echo "N/A")"
                echo ""
                echo -e "${BOLD}${CYAN}  ┌─────────────────────────────────────────────────────────┐${NC}"
                echo -e "${BOLD}${CYAN}  │  🤖 Glue's Auto-Recommended Rules                      │${NC}"
                echo -e "${BOLD}${CYAN}  └─────────────────────────────────────────────────────────┘${NC}"
                echo ""
                echo "${rec_ruleset}" | head -50
                echo ""
                success "Recommendation run completed"
                info "Compare these with our hand-crafted rules in ${GLUE_DQ_RULESET_NAME}"
                ((CREATED++)) || true
                return 0
                ;;
            FAILED|TIMEOUT|ERROR|STOPPED)
                local error_str
                error_str="$(echo "${rec_details}" | python3 -c "import sys,json; print(json.load(sys.stdin).get('ErrorString','No error details'))" 2>/dev/null || echo "Unknown")"
                warn "Recommendation run ${rec_status}: ${error_str}"
                warn "This is optional — proceeding without recommendations"
                return 0
                ;;
            *)
                sleep 15
                elapsed=$((elapsed + 15))
                info "  Status: ${rec_status} (${elapsed}s elapsed)..."
                ;;
        esac
    done

    warn "Recommendation run did not complete within ${max_wait}s — check console"
    info "RunId: ${rec_run_id}"
}

# ═══════════════════════════════════════════════════════════════════════════════
# STEP 7: IAM ROLE FOR LAMBDA
# ═══════════════════════════════════════════════════════════════════════════════
create_lambda_iam_role() {
    step "7" "IAM Role for Lambda — ${LAMBDA_ROLE_NAME}"

    if ${DRY_RUN}; then
        dryrun "Create IAM role ${LAMBDA_ROLE_NAME} with lambda.amazonaws.com trust"
        dryrun "Attach AWSLambdaBasicExecutionRole managed policy"
        dryrun "Attach inline policy for S3 read access to ${BUCKET_NAME}"
        return 0
    fi

    if aws iam get-role --role-name "${LAMBDA_ROLE_NAME}" --no-cli-pager 2>/dev/null; then
        skip "IAM role ${LAMBDA_ROLE_NAME} already exists"
        ((SKIPPED++)) || true
    else
        info "Creating IAM role ${LAMBDA_ROLE_NAME}..."

        local trust_policy
        trust_policy=$(cat <<'TRUST_EOF'
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Principal": {
                "Service": "lambda.amazonaws.com"
            },
            "Action": "sts:AssumeRole"
        }
    ]
}
TRUST_EOF
        )

        aws iam create-role \
            --role-name "${LAMBDA_ROLE_NAME}" \
            --assume-role-policy-document "${trust_policy}" \
            --description "Lambda execution role for healthcare clinical validator" \
            --tags "Key=Project,Value=${TAG_PROJECT}" "Key=Demo,Value=${TAG_DEMO}" \
            --no-cli-pager

        # Attach AWS managed basic execution role (CloudWatch Logs)
        aws iam attach-role-policy \
            --role-name "${LAMBDA_ROLE_NAME}" \
            --policy-arn "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole" \
            --no-cli-pager

        # Inline policy for S3 read access
        local s3_policy
        s3_policy=$(cat <<S3_EOF
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "s3:GetObject",
                "s3:ListBucket"
            ],
            "Resource": [
                "arn:aws:s3:::${BUCKET_NAME}",
                "arn:aws:s3:::${BUCKET_NAME}/*"
            ]
        }
    ]
}
S3_EOF
        )

        aws iam put-role-policy \
            --role-name "${LAMBDA_ROLE_NAME}" \
            --policy-name "S3ReadAccess-${BUCKET_NAME}" \
            --policy-document "${s3_policy}" \
            --no-cli-pager

        success "IAM role ${LAMBDA_ROLE_NAME} created with AWSLambdaBasicExecutionRole + S3 read"
        ((CREATED++)) || true

        # Wait for IAM propagation
        info "Waiting 10s for IAM role propagation..."
        sleep 10
    fi
}

# ═══════════════════════════════════════════════════════════════════════════════
# STEP 8: LAMBDA FUNCTION
# ═══════════════════════════════════════════════════════════════════════════════
deploy_lambda_function() {
    step "8" "Lambda Function — ${LAMBDA_FUNCTION_NAME}"

    local source_file="${DEMO_DIR}/lambda_validation/clinical_validator.py"
    local zip_file="${SCRIPT_DIR}/lambda_package.zip"
    local role_arn="arn:aws:iam::${AWS_ACCOUNT_ID}:role/${LAMBDA_ROLE_NAME}"

    if ${DRY_RUN}; then
        dryrun "Package ${source_file} → ${zip_file}"
        dryrun "Create Lambda function ${LAMBDA_FUNCTION_NAME}"
        return 0
    fi

    if ! [[ -f "${source_file}" ]]; then
        error "Lambda source not found at ${source_file}"
        ((ERRORS++)) || true
        return 1
    fi

    # Package the Lambda function
    info "Packaging Lambda function..."
    (cd "${DEMO_DIR}/lambda_validation" && zip -j "${zip_file}" clinical_validator.py) > /dev/null 2>&1
    success "Lambda package created: ${zip_file}"

    # Check if function exists
    if aws lambda get-function \
        --function-name "${LAMBDA_FUNCTION_NAME}" \
        --region "${AWS_REGION}" \
        --no-cli-pager 2>/dev/null; then
        if ${UPDATE}; then
            info "Updating Lambda function code..."
            aws lambda update-function-code \
                --function-name "${LAMBDA_FUNCTION_NAME}" \
                --zip-file "fileb://${zip_file}" \
                --region "${AWS_REGION}" \
                --no-cli-pager > /dev/null
            success "Lambda function ${LAMBDA_FUNCTION_NAME} code updated"
            ((UPDATED++)) || true
        else
            skip "Lambda function ${LAMBDA_FUNCTION_NAME} already exists (use --update to update code)"
            ((SKIPPED++)) || true
        fi
    else
        info "Creating Lambda function ${LAMBDA_FUNCTION_NAME}..."
        aws lambda create-function \
            --function-name "${LAMBDA_FUNCTION_NAME}" \
            --runtime "${LAMBDA_RUNTIME}" \
            --handler "${LAMBDA_HANDLER}" \
            --role "${role_arn}" \
            --zip-file "fileb://${zip_file}" \
            --memory-size "${LAMBDA_MEMORY}" \
            --timeout "${LAMBDA_TIMEOUT}" \
            --environment "Variables={VALIDATION_LEVEL=full,LOG_LEVEL=INFO}" \
            --tags "Project=${TAG_PROJECT},Demo=${TAG_DEMO}" \
            --description "Clinical domain validator for healthcare patient records" \
            --region "${AWS_REGION}" \
            --no-cli-pager > /dev/null

        success "Lambda function ${LAMBDA_FUNCTION_NAME} created"
        ((CREATED++)) || true
    fi

    # Clean up zip
    rm -f "${zip_file}"
}

# ═══════════════════════════════════════════════════════════════════════════════
# STEP 9: CLOUDWATCH DASHBOARD & ALARMS
# ═══════════════════════════════════════════════════════════════════════════════
deploy_cloudwatch() {
    step "9" "CloudWatch Dashboard & Alarms"

    local dashboard_file="${DEMO_DIR}/cloudwatch/output/cloudwatch_dashboard.json"
    local alarms_file="${DEMO_DIR}/cloudwatch/output/cloudwatch_alarms.json"

    if ${DRY_RUN}; then
        dryrun "Put CloudWatch dashboard ${CW_DASHBOARD_NAME}"
        dryrun "Put ${#CW_ALARM_NAMES[@]} CloudWatch alarms"
        return 0
    fi

    # --- Dashboard (put-dashboard is idempotent — always upserts) ---
    if ! [[ -f "${dashboard_file}" ]]; then
        error "Dashboard config not found at ${dashboard_file}. Run quality_dashboard.py first."
        ((ERRORS++)) || true
        return 1
    fi

    info "Deploying CloudWatch dashboard ${CW_DASHBOARD_NAME}..."

    # Extract DashboardBody from the JSON file
    local dashboard_body
    dashboard_body="$(python3 -c "
import json, sys
with open('${dashboard_file}') as f:
    data = json.load(f)
print(data['DashboardBody'])
")"

    aws cloudwatch put-dashboard \
        --dashboard-name "${CW_DASHBOARD_NAME}" \
        --dashboard-body "${dashboard_body}" \
        --region "${AWS_REGION}" \
        --no-cli-pager > /dev/null

    success "CloudWatch dashboard ${CW_DASHBOARD_NAME} deployed"
    ((UPDATED++)) || true

    # --- Alarms (put-metric-alarm is idempotent — always upserts) ---
    if ! [[ -f "${alarms_file}" ]]; then
        error "Alarms config not found at ${alarms_file}. Run quality_dashboard.py first."
        ((ERRORS++)) || true
        return 1
    fi

    info "Deploying CloudWatch alarms..."

    # Read the number of alarms
    local num_alarms
    num_alarms="$(python3 -c "
import json
with open('${alarms_file}') as f:
    alarms = json.load(f)
print(len(alarms))
")"

    local i
    for ((i = 0; i < num_alarms; i++)); do
        # Extract each alarm config and build the CLI command
        local alarm_name alarm_desc namespace metric_name statistic period eval_periods
        local threshold comparison treat_missing

        alarm_name="$(python3 -c "
import json
with open('${alarms_file}') as f:
    alarms = json.load(f)
print(alarms[${i}]['AlarmName'])
")"
        alarm_desc="$(python3 -c "
import json
with open('${alarms_file}') as f:
    alarms = json.load(f)
print(alarms[${i}]['AlarmDescription'])
")"
        namespace="$(python3 -c "
import json
with open('${alarms_file}') as f:
    alarms = json.load(f)
print(alarms[${i}]['Namespace'])
")"
        metric_name="$(python3 -c "
import json
with open('${alarms_file}') as f:
    alarms = json.load(f)
print(alarms[${i}]['MetricName'])
")"
        statistic="$(python3 -c "
import json
with open('${alarms_file}') as f:
    alarms = json.load(f)
print(alarms[${i}]['Statistic'])
")"
        period="$(python3 -c "
import json
with open('${alarms_file}') as f:
    alarms = json.load(f)
print(alarms[${i}]['Period'])
")"
        eval_periods="$(python3 -c "
import json
with open('${alarms_file}') as f:
    alarms = json.load(f)
print(alarms[${i}]['EvaluationPeriods'])
")"
        threshold="$(python3 -c "
import json
with open('${alarms_file}') as f:
    alarms = json.load(f)
print(alarms[${i}]['Threshold'])
")"
        comparison="$(python3 -c "
import json
with open('${alarms_file}') as f:
    alarms = json.load(f)
print(alarms[${i}]['ComparisonOperator'])
")"
        treat_missing="$(python3 -c "
import json
with open('${alarms_file}') as f:
    alarms = json.load(f)
print(alarms[${i}]['TreatMissingData'])
")"

        # Build dimensions argument
        local dimensions_args
        dimensions_args="$(python3 -c "
import json
with open('${alarms_file}') as f:
    alarms = json.load(f)
dims = alarms[${i}]['Dimensions']
parts = []
for d in dims:
    parts.append(f\"Name={d['Name']},Value={d['Value']}\")
print(' '.join(parts))
")"

        # Replace ACCOUNT_ID in alarm actions with real account
        local alarm_actions_arg=""
        local has_actions
        has_actions="$(python3 -c "
import json
with open('${alarms_file}') as f:
    alarms = json.load(f)
actions = alarms[${i}].get('AlarmActions', [])
print('yes' if actions else 'no')
")"
        if [[ "${has_actions}" == "yes" ]]; then
            local action_arn
            action_arn="$(python3 -c "
import json
with open('${alarms_file}') as f:
    alarms = json.load(f)
action = alarms[${i}]['AlarmActions'][0]
print(action.replace('ACCOUNT_ID', '${AWS_ACCOUNT_ID}'))
")"
            # Only add alarm-actions if the SNS topic actually exists; skip otherwise
            # to avoid errors. The alarm will be created without actions.
            alarm_actions_arg=""
            warn "Alarm action SNS topic may not exist — alarm created without actions"
        fi

        aws cloudwatch put-metric-alarm \
            --alarm-name "${alarm_name}" \
            --alarm-description "${alarm_desc}" \
            --namespace "${namespace}" \
            --metric-name "${metric_name}" \
            --dimensions ${dimensions_args} \
            --statistic "${statistic}" \
            --period "${period}" \
            --evaluation-periods "${eval_periods}" \
            --threshold "${threshold}" \
            --comparison-operator "${comparison}" \
            --treat-missing-data "${treat_missing}" \
            --region "${AWS_REGION}" \
            --no-cli-pager

        success "Alarm deployed: ${alarm_name}"
    done
    ((UPDATED++)) || true
}

# ═══════════════════════════════════════════════════════════════════════════════
# CLEANUP / TEARDOWN
# ═══════════════════════════════════════════════════════════════════════════════
cleanup_all() {
    header "TEARDOWN — Removing all Demo 1 resources"
    warn "This will permanently delete all resources. Proceeding in 5 seconds..."
    sleep 5

    # --- Lambda Function ---
    info "Deleting Lambda function ${LAMBDA_FUNCTION_NAME}..."
    if aws lambda get-function --function-name "${LAMBDA_FUNCTION_NAME}" --region "${AWS_REGION}" --no-cli-pager 2>/dev/null; then
        aws lambda delete-function \
            --function-name "${LAMBDA_FUNCTION_NAME}" \
            --region "${AWS_REGION}" \
            --no-cli-pager
        success "Lambda function deleted"
        ((DELETED++)) || true
    else
        skip "Lambda function ${LAMBDA_FUNCTION_NAME} does not exist"
    fi

    # --- Lambda CloudWatch Log Group ---
    local lambda_log_group="/aws/lambda/${LAMBDA_FUNCTION_NAME}"
    info "Deleting CloudWatch log group ${lambda_log_group}..."
    if aws logs describe-log-groups \
        --log-group-name-prefix "${lambda_log_group}" \
        --region "${AWS_REGION}" \
        --query "logGroups[?logGroupName=='${lambda_log_group}'].logGroupName" \
        --output text \
        --no-cli-pager 2>/dev/null | grep -q "${LAMBDA_FUNCTION_NAME}"; then
        aws logs delete-log-group \
            --log-group-name "${lambda_log_group}" \
            --region "${AWS_REGION}" \
            --no-cli-pager
        success "CloudWatch log group deleted: ${lambda_log_group}"
        ((DELETED++)) || true
    else
        skip "CloudWatch log group ${lambda_log_group} does not exist"
    fi

    # --- CloudWatch Alarms ---
    info "Deleting CloudWatch alarms..."
    for alarm_name in "${CW_ALARM_NAMES[@]}"; do
        if aws cloudwatch describe-alarms \
            --alarm-names "${alarm_name}" \
            --region "${AWS_REGION}" \
            --query "MetricAlarms[0].AlarmName" \
            --output text \
            --no-cli-pager 2>/dev/null | grep -q "${alarm_name}"; then
            aws cloudwatch delete-alarms \
                --alarm-names "${alarm_name}" \
                --region "${AWS_REGION}" \
                --no-cli-pager
            success "Alarm deleted: ${alarm_name}"
            ((DELETED++)) || true
        else
            skip "Alarm ${alarm_name} does not exist"
        fi
    done

    # --- CloudWatch Dashboard ---
    info "Deleting CloudWatch dashboard ${CW_DASHBOARD_NAME}..."
    if aws cloudwatch get-dashboard \
        --dashboard-name "${CW_DASHBOARD_NAME}" \
        --region "${AWS_REGION}" \
        --no-cli-pager 2>/dev/null; then
        aws cloudwatch delete-dashboards \
            --dashboard-names "${CW_DASHBOARD_NAME}" \
            --region "${AWS_REGION}" \
            --no-cli-pager
        success "Dashboard deleted: ${CW_DASHBOARD_NAME}"
        ((DELETED++)) || true
    else
        skip "Dashboard ${CW_DASHBOARD_NAME} does not exist"
    fi

    # --- Cancel any running DQ evaluation or recommendation runs ---
    # Check for runs across all known tables
    local all_tables
    all_tables="$(aws glue get-tables \
        --database-name "${GLUE_DB_NAME}" \
        --region "${AWS_REGION}" \
        --query "TableList[].Name" \
        --output text \
        --no-cli-pager 2>/dev/null || echo "")"

    local tables_to_check="${all_tables:-${DQ_TABLE_NAMES[*]}}"

    for cleanup_table in ${tables_to_check}; do
        [[ "${cleanup_table}" == "None" || -z "${cleanup_table}" ]] && continue

        info "Checking for running DQ runs on ${cleanup_table}..."
        local running_eval_runs
        running_eval_runs="$(aws glue list-data-quality-ruleset-evaluation-runs \
            --filter "{\"DataSource\": {\"GlueTable\": {\"DatabaseName\": \"${GLUE_DB_NAME}\", \"TableName\": \"${cleanup_table}\"}}}" \
            --region "${AWS_REGION}" \
            --query "Runs[?Status=='RUNNING' || Status=='STARTING'].RunId" \
            --output text \
            --no-cli-pager 2>/dev/null || echo "")"
        if [[ -n "${running_eval_runs}" && "${running_eval_runs}" != "None" ]]; then
            for eval_run_id in ${running_eval_runs}; do
                info "Cancelling DQ evaluation run: ${eval_run_id}"
                aws glue cancel-data-quality-ruleset-evaluation-run \
                    --run-id "${eval_run_id}" --region "${AWS_REGION}" --no-cli-pager 2>/dev/null || true
            done
            success "Cancelled running DQ evaluation runs on ${cleanup_table}"
        fi

        local running_rec_runs
        running_rec_runs="$(aws glue list-data-quality-rule-recommendation-runs \
            --filter "{\"DataSource\": {\"GlueTable\": {\"DatabaseName\": \"${GLUE_DB_NAME}\", \"TableName\": \"${cleanup_table}\"}}}" \
            --region "${AWS_REGION}" \
            --query "Runs[?Status=='RUNNING' || Status=='STARTING'].RunId" \
            --output text \
            --no-cli-pager 2>/dev/null || echo "")"
        if [[ -n "${running_rec_runs}" && "${running_rec_runs}" != "None" ]]; then
            for rec_run_id in ${running_rec_runs}; do
                info "Cancelling DQ recommendation run: ${rec_run_id}"
                aws glue cancel-data-quality-rule-recommendation-run \
                    --run-id "${rec_run_id}" --region "${AWS_REGION}" --no-cli-pager 2>/dev/null || true
            done
            success "Cancelled running DQ recommendation runs on ${cleanup_table}"
        fi
    done

    # --- Glue DQ Rulesets (all 3 + legacy) ---
    local all_ruleset_names=("${DQ_RULESET_NAMES[@]}" "${GLUE_DQ_RULESET_NAME_LEGACY}")
    for rs_name in "${all_ruleset_names[@]}"; do
        [[ -z "${rs_name}" ]] && continue
        info "Deleting Glue DQ ruleset ${rs_name}..."
        if aws glue get-data-quality-ruleset --name "${rs_name}" --region "${AWS_REGION}" --no-cli-pager 2>/dev/null; then
            aws glue delete-data-quality-ruleset --name "${rs_name}" --region "${AWS_REGION}" --no-cli-pager
            success "Glue DQ ruleset deleted: ${rs_name}"
            ((DELETED++)) || true
        else
            skip "Glue DQ ruleset ${rs_name} does not exist"
        fi
    done

    # --- Glue Crawler (stop if running, then delete) ---
    info "Deleting Glue crawler ${GLUE_CRAWLER_NAME}..."
    if aws glue get-crawler --name "${GLUE_CRAWLER_NAME}" --region "${AWS_REGION}" --no-cli-pager 2>/dev/null; then
        local crawler_state
        crawler_state="$(aws glue get-crawler \
            --name "${GLUE_CRAWLER_NAME}" \
            --region "${AWS_REGION}" \
            --query "Crawler.State" \
            --output text \
            --no-cli-pager)"
        if [[ "${crawler_state}" == "RUNNING" ]]; then
            info "Stopping running crawler..."
            aws glue stop-crawler \
                --name "${GLUE_CRAWLER_NAME}" \
                --region "${AWS_REGION}" \
                --no-cli-pager
            sleep 10
        fi
        aws glue delete-crawler \
            --name "${GLUE_CRAWLER_NAME}" \
            --region "${AWS_REGION}" \
            --no-cli-pager
        success "Glue crawler deleted"
        ((DELETED++)) || true
    else
        skip "Glue crawler ${GLUE_CRAWLER_NAME} does not exist"
    fi

    # --- Glue Tables (BEFORE revoking LF permissions) ---
    # CRITICAL: Must delete tables BEFORE revoking the caller's TABLE wildcard LF permissions.
    # If the caller is not a Lake Formation admin, revoking TABLE wildcard first would make
    # tables invisible (get-tables returns empty), leaving orphaned tables in the database.
    # See AWS_AI_LEARNINGS.md entry #11.
    info "Deleting Glue tables in ${GLUE_DB_NAME} (before LF revocation)..."
    if aws glue get-database --name "${GLUE_DB_NAME}" --region "${AWS_REGION}" --no-cli-pager 2>/dev/null; then
        # Build a combined list: discovered tables + known table names (as fallback)
        local tables_to_delete="${all_tables:-}"
        for known_table in "${DQ_TABLE_NAMES[@]}"; do
            if [[ -z "${tables_to_delete}" ]] || ! echo "${tables_to_delete}" | grep -qw "${known_table}"; then
                tables_to_delete="${tables_to_delete} ${known_table}"
            fi
        done
        for tbl in ${tables_to_delete}; do
            [[ "${tbl}" == "None" || -z "${tbl}" ]] && continue
            if aws glue get-table --database-name "${GLUE_DB_NAME}" --name "${tbl}" \
                --region "${AWS_REGION}" --no-cli-pager 2>/dev/null; then
                aws glue delete-table \
                    --database-name "${GLUE_DB_NAME}" \
                    --name "${tbl}" \
                    --region "${AWS_REGION}" \
                    --no-cli-pager 2>/dev/null || warn "Could not delete table ${tbl}"
                info "  Deleted table: ${tbl}"
                ((DELETED++)) || true
            fi
        done
    else
        skip "Glue database ${GLUE_DB_NAME} does not exist — no tables to delete"
    fi

    # --- Lake Formation Permissions (DATABASE + TABLE) ---
    info "Revoking Lake Formation permissions for ${GLUE_CRAWLER_ROLE_NAME} on ${GLUE_DB_NAME}..."
    local lf_role_arn="arn:aws:iam::${AWS_ACCOUNT_ID}:role/${GLUE_CRAWLER_ROLE_NAME}"

    # Revoke TABLE wildcard permissions first (Glue role)
    local lf_table_existing
    lf_table_existing=$(aws lakeformation list-permissions \
        --principal "DataLakePrincipalIdentifier=${lf_role_arn}" \
        --resource-type "TABLE" \
        --resource '{"Table": {"DatabaseName": "'"${GLUE_DB_NAME}"'", "TableWildcard": {}}}' \
        --region "${AWS_REGION}" \
        --query "PrincipalResourcePermissions" \
        --output json \
        --no-cli-pager 2>/dev/null || echo "[]")
    if [[ -n "${lf_table_existing}" && "${lf_table_existing}" != "[]" ]]; then
        aws lakeformation revoke-permissions \
            --principal "DataLakePrincipalIdentifier=${lf_role_arn}" \
            --permissions "ALL" "SELECT" "ALTER" "DROP" "DELETE" "INSERT" "DESCRIBE" \
            --permissions-with-grant-option "ALL" "SELECT" "ALTER" "DROP" "DELETE" "INSERT" "DESCRIBE" \
            --resource '{"Table": {"DatabaseName": "'"${GLUE_DB_NAME}"'", "TableWildcard": {}}}' \
            --region "${AWS_REGION}" \
            --no-cli-pager 2>/dev/null || warn "Could not revoke TABLE wildcard LF permissions for Glue role"
        success "Lake Formation TABLE wildcard permissions revoked for Glue role"
        ((DELETED++)) || true
    fi

    # Revoke TABLE wildcard permissions for caller role
    local caller_arn
    caller_arn="$(aws sts get-caller-identity --region "${AWS_REGION}" --query "Arn" --output text --no-cli-pager)"
    local caller_role_arn=""
    if [[ "${caller_arn}" == *":assumed-role/"* ]]; then
        local caller_role_name
        caller_role_name="$(echo "${caller_arn}" | sed 's|.*:assumed-role/\([^/]*\)/.*|\1|')"
        caller_role_arn="arn:aws:iam::${AWS_ACCOUNT_ID}:role/${caller_role_name}"
    elif [[ "${caller_arn}" == *":role/"* ]]; then
        caller_role_arn="${caller_arn}"
    elif [[ "${caller_arn}" == *":user/"* ]]; then
        caller_role_arn="${caller_arn}"
    fi
    if [[ -n "${caller_role_arn}" && "${caller_role_arn}" != "${lf_role_arn}" ]]; then
        local caller_table_existing
        caller_table_existing=$(aws lakeformation list-permissions \
            --principal "DataLakePrincipalIdentifier=${caller_role_arn}" \
            --resource-type "TABLE" \
            --resource '{"Table": {"DatabaseName": "'"${GLUE_DB_NAME}"'", "TableWildcard": {}}}' \
            --region "${AWS_REGION}" \
            --query "PrincipalResourcePermissions" \
            --output json \
            --no-cli-pager 2>/dev/null || echo "[]")
        if [[ -n "${caller_table_existing}" && "${caller_table_existing}" != "[]" ]]; then
            aws lakeformation revoke-permissions \
                --principal "DataLakePrincipalIdentifier=${caller_role_arn}" \
                --permissions "ALL" "SELECT" "ALTER" "DROP" "DELETE" "INSERT" "DESCRIBE" \
                --permissions-with-grant-option "ALL" "SELECT" "ALTER" "DROP" "DELETE" "INSERT" "DESCRIBE" \
                --resource '{"Table": {"DatabaseName": "'"${GLUE_DB_NAME}"'", "TableWildcard": {}}}' \
                --region "${AWS_REGION}" \
                --no-cli-pager 2>/dev/null || warn "Could not revoke TABLE wildcard LF permissions for caller role"
            success "Lake Formation TABLE wildcard permissions revoked for caller role"
            ((DELETED++)) || true
        fi
    fi

    # Revoke DATABASE-level permissions (Glue role)
    local lf_existing
    lf_existing=$(aws lakeformation list-permissions \
        --principal "DataLakePrincipalIdentifier=${lf_role_arn}" \
        --resource-type "DATABASE" \
        --resource '{"Database": {"Name": "'"${GLUE_DB_NAME}"'"}}' \
        --region "${AWS_REGION}" \
        --query "PrincipalResourcePermissions" \
        --output json \
        --no-cli-pager 2>/dev/null || echo "[]")
    if [[ -n "${lf_existing}" && "${lf_existing}" != "[]" ]]; then
        aws lakeformation revoke-permissions \
            --principal "DataLakePrincipalIdentifier=${lf_role_arn}" \
            --permissions "CREATE_TABLE" "DESCRIBE" "ALTER" "DROP" \
            --resource '{"Database": {"Name": "'"${GLUE_DB_NAME}"'"}}' \
            --region "${AWS_REGION}" \
            --no-cli-pager 2>/dev/null || warn "Could not revoke Lake Formation DATABASE permissions (may already be removed)"
        success "Lake Formation DATABASE permissions revoked"
        ((DELETED++)) || true
    else
        skip "No Lake Formation permissions found for ${GLUE_CRAWLER_ROLE_NAME} on ${GLUE_DB_NAME}"
    fi

    # --- Glue Database (tables already deleted above) ---
    info "Deleting Glue database ${GLUE_DB_NAME}..."
    if aws glue get-database --name "${GLUE_DB_NAME}" --region "${AWS_REGION}" --no-cli-pager 2>/dev/null; then
        # Safety net: delete any remaining tables (e.g., created after our earlier pass)
        local remaining_tables
        remaining_tables="$(aws glue get-tables \
            --database-name "${GLUE_DB_NAME}" \
            --region "${AWS_REGION}" \
            --query "TableList[].Name" \
            --output text \
            --no-cli-pager 2>/dev/null || echo "")"
        if [[ -n "${remaining_tables}" && "${remaining_tables}" != "None" ]]; then
            for tbl in ${remaining_tables}; do
                aws glue delete-table \
                    --database-name "${GLUE_DB_NAME}" \
                    --name "${tbl}" \
                    --region "${AWS_REGION}" \
                    --no-cli-pager 2>/dev/null || warn "Could not delete remaining table ${tbl}"
                info "  Deleted remaining table: ${tbl}"
            done
        fi
        aws glue delete-database \
            --name "${GLUE_DB_NAME}" \
            --region "${AWS_REGION}" \
            --no-cli-pager
        success "Glue database deleted"
        ((DELETED++)) || true
    else
        skip "Glue database ${GLUE_DB_NAME} does not exist"
    fi

    # --- IAM Role: Lambda ---
    info "Deleting IAM role ${LAMBDA_ROLE_NAME}..."
    if aws iam get-role --role-name "${LAMBDA_ROLE_NAME}" --no-cli-pager 2>/dev/null; then
        # Detach managed policies
        local policies
        policies="$(aws iam list-attached-role-policies \
            --role-name "${LAMBDA_ROLE_NAME}" \
            --query "AttachedPolicies[].PolicyArn" \
            --output text \
            --no-cli-pager 2>/dev/null || echo "")"
        for pol_arn in ${policies}; do
            [[ "${pol_arn}" == "None" || -z "${pol_arn}" ]] && continue
            aws iam detach-role-policy \
                --role-name "${LAMBDA_ROLE_NAME}" \
                --policy-arn "${pol_arn}" \
                --no-cli-pager
        done
        # Delete inline policies
        local inline_pols
        inline_pols="$(aws iam list-role-policies \
            --role-name "${LAMBDA_ROLE_NAME}" \
            --query "PolicyNames[]" \
            --output text \
            --no-cli-pager 2>/dev/null || echo "")"
        for ip in ${inline_pols}; do
            [[ "${ip}" == "None" || -z "${ip}" ]] && continue
            aws iam delete-role-policy \
                --role-name "${LAMBDA_ROLE_NAME}" \
                --policy-name "${ip}" \
                --no-cli-pager
        done
        aws iam delete-role --role-name "${LAMBDA_ROLE_NAME}" --no-cli-pager
        success "IAM role ${LAMBDA_ROLE_NAME} deleted"
        ((DELETED++)) || true
    else
        skip "IAM role ${LAMBDA_ROLE_NAME} does not exist"
    fi

    # --- IAM Role: Glue ---
    info "Deleting IAM role ${GLUE_CRAWLER_ROLE_NAME}..."
    if aws iam get-role --role-name "${GLUE_CRAWLER_ROLE_NAME}" --no-cli-pager 2>/dev/null; then
        local policies
        policies="$(aws iam list-attached-role-policies \
            --role-name "${GLUE_CRAWLER_ROLE_NAME}" \
            --query "AttachedPolicies[].PolicyArn" \
            --output text \
            --no-cli-pager 2>/dev/null || echo "")"
        for pol_arn in ${policies}; do
            [[ "${pol_arn}" == "None" || -z "${pol_arn}" ]] && continue
            aws iam detach-role-policy \
                --role-name "${GLUE_CRAWLER_ROLE_NAME}" \
                --policy-arn "${pol_arn}" \
                --no-cli-pager
        done
        local inline_pols
        inline_pols="$(aws iam list-role-policies \
            --role-name "${GLUE_CRAWLER_ROLE_NAME}" \
            --query "PolicyNames[]" \
            --output text \
            --no-cli-pager 2>/dev/null || echo "")"
        for ip in ${inline_pols}; do
            [[ "${ip}" == "None" || -z "${ip}" ]] && continue
            aws iam delete-role-policy \
                --role-name "${GLUE_CRAWLER_ROLE_NAME}" \
                --policy-name "${ip}" \
                --no-cli-pager
        done
        aws iam delete-role --role-name "${GLUE_CRAWLER_ROLE_NAME}" --no-cli-pager
        success "IAM role ${GLUE_CRAWLER_ROLE_NAME} deleted"
        ((DELETED++)) || true
    else
        skip "IAM role ${GLUE_CRAWLER_ROLE_NAME} does not exist"
    fi

    # --- S3 Bucket (empty first, then delete) ---
    info "Deleting S3 bucket ${BUCKET_NAME}..."
    if aws s3api head-bucket --bucket "${BUCKET_NAME}" --region "${AWS_REGION}" 2>/dev/null; then
        info "Emptying bucket first..."
        aws s3 rm "s3://${BUCKET_NAME}" --recursive --region "${AWS_REGION}" --no-cli-pager
        aws s3api delete-bucket \
            --bucket "${BUCKET_NAME}" \
            --region "${AWS_REGION}" \
            --no-cli-pager
        success "S3 bucket ${BUCKET_NAME} deleted"
        ((DELETED++)) || true
    else
        skip "S3 bucket ${BUCKET_NAME} does not exist"
    fi

    # Clean up local artifacts
    rm -f "${SCRIPT_DIR}/lambda_package.zip"
}

# ═══════════════════════════════════════════════════════════════════════════════
# SUMMARY
# ═══════════════════════════════════════════════════════════════════════════════
print_summary() {
    echo ""
    header "Summary"
    echo -e "  ${GREEN}Created:${NC}  ${CREATED}"
    echo -e "  ${YELLOW}Skipped:${NC}  ${SKIPPED}"
    echo -e "  ${BLUE}Updated:${NC}  ${UPDATED}"
    echo -e "  ${RED}Deleted:${NC}  ${DELETED}"
    echo -e "  ${RED}Errors:${NC}   ${ERRORS}"
    echo ""
    echo -e "  ${BOLD}Resources:${NC}"
    echo -e "    S3 Bucket:         ${BUCKET_NAME}"
    echo -e "    Glue Database:     ${GLUE_DB_NAME}"
    echo -e "    Glue Crawler:      ${GLUE_CRAWLER_NAME}"
    echo -e "    Glue DQ Rulesets:   ${DQ_RULESET_NAMES[*]}"
    echo -e "    Glue DQ Tables:    ${DQ_TABLE_NAMES[*]}"
    echo -e "    Glue DQ Eval Run:  (Step 6b — serverless evaluation with CW metrics)"
    echo -e "    Glue DQ Recommend: (Step 6c — auto-generated rule suggestions)"
    echo -e "    IAM Role (Glue):   ${GLUE_CRAWLER_ROLE_NAME}"
    echo -e "    IAM Role (Lambda): ${LAMBDA_ROLE_NAME}"
    echo -e "    Lambda Function:   ${LAMBDA_FUNCTION_NAME}"
    echo -e "    CW Dashboard:      ${CW_DASHBOARD_NAME}"
    echo -e "    CW Alarms:         ${CW_ALARM_NAMES[*]}"
    echo ""
    echo -e "  Region:   ${AWS_REGION}"
    echo -e "  Log file: ${LOG_FILE}"
    echo ""
}

# ═══════════════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════════════
main() {
    # Initialize log
    mkdir -p "$(dirname "${LOG_FILE}")"
    echo "" >> "${LOG_FILE}"
    log "════════════════════════════════════════════════════════════"
    log "Script started: DRY_RUN=${DRY_RUN} CLEANUP=${CLEANUP} UPDATE=${UPDATE}"

    if ${CLEANUP}; then
        check_prerequisites
        progress "STARTED" "Cleanup/teardown of all AWS resources"
        cleanup_all
        print_summary
        progress "COMPLETED" "Teardown complete — deleted ${DELETED} resources"
        return 0
    fi

    if ${DRY_RUN}; then
        header "DRY RUN — No resources will be created"
    else
        header "Demo 1: Healthcare Records — AWS Infrastructure Setup"
    fi

    check_prerequisites
    progress "STARTED" "Infrastructure setup (dry_run=${DRY_RUN}, update=${UPDATE})"

    # Ensure local outputs exist
    ensure_synth_data
    ensure_glue_dq_outputs
    ensure_cloudwatch_outputs

    # Provision AWS resources
    create_s3_bucket
    upload_data
    create_glue_iam_role
    create_glue_database
    grant_lakeformation_permissions
    create_glue_crawler
    create_glue_dq_rulesets
    run_dq_evaluations
    run_dq_recommendation
    create_lambda_iam_role
    deploy_lambda_function
    deploy_cloudwatch

    print_summary

    if ${DRY_RUN}; then
        progress "COMPLETED" "Dry run finished — no resources created"
    else
        progress "COMPLETED" "Infrastructure setup complete — created=${CREATED} skipped=${SKIPPED} updated=${UPDATED} errors=${ERRORS}"
    fi
}

main "$@"
