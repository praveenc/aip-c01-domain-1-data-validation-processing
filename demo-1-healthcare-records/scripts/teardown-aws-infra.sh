#!/usr/bin/env bash
#
# teardown-aws-infra.sh — Convenience wrapper that calls setup-aws-infra.sh --cleanup
#
# Tears down all Demo 1 AWS resources in dependency-safe reverse order:
#
#   1. Lambda function (healthcare-clinical-validator)
#   2. Lambda CloudWatch log group (/aws/lambda/healthcare-clinical-validator)
#   3. CloudWatch alarms (3: OverallScore-Low, HighErrorRate, DrugInteraction-Alert)
#   4. CloudWatch dashboard (Healthcare-Data-Quality)
#   5. Cancel running DQ evaluation + recommendation runs (all tables)
#   6. Glue DQ rulesets (3 per-table + 1 legacy)
#   7. Glue crawler (stop if running, then delete)
#   8. Glue tables (3 crawler-created — BEFORE LF revocation to avoid invisible tables)
#   9. Lake Formation TABLE wildcard permissions (Glue role + caller role)
#  10. Lake Formation DATABASE permissions (Glue role)
#  11. Glue database (safety-net table check, then delete)
#  12. IAM role: LambdaRole-HealthcareValidator (detach managed + delete inline + delete role)
#  13. IAM role: AWSGlueServiceRole-HealthRecords (detach managed + delete inline + delete role)
#  14. S3 bucket: demo-1-healthcare-records-pdx (empty all objects, then delete)
#  15. Local artifact cleanup (lambda_package.zip)
#
# Usage:
#   ./teardown-aws-infra.sh              # Tear down all resources
#   ./teardown-aws-infra.sh --dry-run    # Show what --cleanup would do (not supported — exits)
#
# Environment:
#   AWS_PROFILE=001      (default)
#   AWS_REGION=us-west-2 (default)
#
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
exec "${SCRIPT_DIR}/setup-aws-infra.sh" --cleanup "$@"
