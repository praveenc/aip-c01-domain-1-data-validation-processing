"""CloudWatch metrics publishing and dashboard setup for data quality monitoring.

Demonstrates:
  - Publishing custom CloudWatch metrics from validation results
  - Creating alarms for quality degradation
  - Building a CloudWatch dashboard for visibility into data quality trends

Covers requirement 1.4: CloudWatch metrics & dashboards for quality monitoring
"""

import json
from datetime import datetime, timezone
from pathlib import Path

OUTPUT_DIR = Path(__file__).parent / "output"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

NAMESPACE = "AIP-C01/HealthcareDataQuality"
DEMO_REGION = "us-east-1"


def build_cloudwatch_metric_data(glue_dq_report: dict, validation_summary: dict) -> list[dict]:
    """Convert validation results into CloudWatch PutMetricData format.

    Args:
        glue_dq_report: Output from glue_dq_rules.py
        validation_summary: Output from clinical_validator.py

    Returns:
        List of metric data entries for cloudwatch.put_metric_data()
    """
    timestamp = datetime.now(timezone.utc).isoformat()

    metrics = []

    # --- Glue DQ Metrics ---
    metrics.append({
        "MetricName": "GlueDQ_OverallScore",
        "Value": glue_dq_report.get("overall_score", 0),
        "Unit": "Percent",
        "Timestamp": timestamp,
        "Dimensions": [
            {"Name": "Dataset", "Value": "patient_records"},
            {"Name": "Pipeline", "Value": "healthcare-ingest"},
        ],
    })
    metrics.append({
        "MetricName": "GlueDQ_RulesPassed",
        "Value": glue_dq_report.get("rules_passed", 0),
        "Unit": "Count",
        "Timestamp": timestamp,
        "Dimensions": [
            {"Name": "Dataset", "Value": "patient_records"},
        ],
    })
    metrics.append({
        "MetricName": "GlueDQ_RulesFailed",
        "Value": glue_dq_report.get("rules_failed", 0),
        "Unit": "Count",
        "Timestamp": timestamp,
        "Dimensions": [
            {"Name": "Dataset", "Value": "patient_records"},
        ],
    })
    metrics.append({
        "MetricName": "GlueDQ_TotalRecords",
        "Value": glue_dq_report.get("total_records", 0),
        "Unit": "Count",
        "Timestamp": timestamp,
        "Dimensions": [
            {"Name": "Dataset", "Value": "patient_records"},
        ],
    })

    # --- Lambda Validation Metrics ---
    metrics.append({
        "MetricName": "Validation_PassRate",
        "Value": (validation_summary.get("passed", 0) / max(validation_summary.get("total", 1), 1)) * 100,
        "Unit": "Percent",
        "Timestamp": timestamp,
        "Dimensions": [
            {"Name": "Validator", "Value": "clinical-validator"},
        ],
    })
    metrics.append({
        "MetricName": "Validation_ErrorCount",
        "Value": validation_summary.get("failed", 0),
        "Unit": "Count",
        "Timestamp": timestamp,
        "Dimensions": [
            {"Name": "Validator", "Value": "clinical-validator"},
        ],
    })
    metrics.append({
        "MetricName": "Validation_WarningCount",
        "Value": validation_summary.get("warnings", 0),
        "Unit": "Count",
        "Timestamp": timestamp,
        "Dimensions": [
            {"Name": "Validator", "Value": "clinical-validator"},
        ],
    })

    # Per-error-type metrics
    for error_type, count in validation_summary.get("errors_by_type", {}).items():
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

    return metrics


def build_cloudwatch_alarms() -> list[dict]:
    """Define CloudWatch alarms for quality degradation detection.

    Returns list of alarm configurations for cloudwatch.put_metric_alarm()
    """
    return [
        {
            "AlarmName": "Healthcare-DQ-OverallScore-Low",
            "AlarmDescription": "Glue DQ overall score dropped below 80%",
            "Namespace": NAMESPACE,
            "MetricName": "GlueDQ_OverallScore",
            "Dimensions": [{"Name": "Dataset", "Value": "patient_records"}],
            "Statistic": "Average",
            "Period": 300,
            "EvaluationPeriods": 2,
            "Threshold": 80.0,
            "ComparisonOperator": "LessThanThreshold",
            "AlarmActions": ["arn:aws:sns:us-east-1:ACCOUNT_ID:data-quality-alerts"],
            "TreatMissingData": "breaching",
        },
        {
            "AlarmName": "Healthcare-Validation-HighErrorRate",
            "AlarmDescription": "Clinical validation error count exceeds threshold",
            "Namespace": NAMESPACE,
            "MetricName": "Validation_ErrorCount",
            "Dimensions": [{"Name": "Validator", "Value": "clinical-validator"}],
            "Statistic": "Sum",
            "Period": 300,
            "EvaluationPeriods": 1,
            "Threshold": 20.0,
            "ComparisonOperator": "GreaterThanThreshold",
            "AlarmActions": ["arn:aws:sns:us-east-1:ACCOUNT_ID:data-quality-alerts"],
            "TreatMissingData": "notBreaching",
        },
        {
            "AlarmName": "Healthcare-DrugInteraction-Alert",
            "AlarmDescription": "Drug interaction warnings detected in batch",
            "Namespace": NAMESPACE,
            "MetricName": "Validation_ErrorsByType",
            "Dimensions": [
                {"Name": "ErrorType", "Value": "DRUG_INTERACTION"},
                {"Name": "Validator", "Value": "clinical-validator"},
            ],
            "Statistic": "Sum",
            "Period": 300,
            "EvaluationPeriods": 1,
            "Threshold": 5.0,
            "ComparisonOperator": "GreaterThanThreshold",
            "AlarmActions": ["arn:aws:sns:us-east-1:ACCOUNT_ID:data-quality-alerts"],
            "TreatMissingData": "notBreaching",
        },
    ]


def build_cloudwatch_dashboard() -> dict:
    """Build a CloudWatch dashboard definition for healthcare data quality.

    Returns the dashboard body for cloudwatch.put_dashboard()
    """
    return {
        "DashboardName": "Healthcare-Data-Quality",
        "DashboardBody": json.dumps({
            "widgets": [
                {
                    "type": "metric",
                    "x": 0, "y": 0, "width": 12, "height": 6,
                    "properties": {
                        "title": "Glue DQ Overall Score (%)",
                        "metrics": [
                            [NAMESPACE, "GlueDQ_OverallScore", "Dataset", "patient_records",
                             {"stat": "Average", "period": 300}]
                        ],
                        "view": "timeSeries",
                        "region": DEMO_REGION,
                        "yAxis": {"left": {"min": 0, "max": 100}},
                        "annotations": {
                            "horizontal": [
                                {"label": "Minimum Acceptable", "value": 80, "color": "#d62728"}
                            ]
                        },
                    },
                },
                {
                    "type": "metric",
                    "x": 12, "y": 0, "width": 12, "height": 6,
                    "properties": {
                        "title": "Validation Pass Rate (%)",
                        "metrics": [
                            [NAMESPACE, "Validation_PassRate", "Validator", "clinical-validator",
                             {"stat": "Average", "period": 300}]
                        ],
                        "view": "timeSeries",
                        "region": DEMO_REGION,
                        "yAxis": {"left": {"min": 0, "max": 100}},
                    },
                },
                {
                    "type": "metric",
                    "x": 0, "y": 6, "width": 8, "height": 6,
                    "properties": {
                        "title": "Glue DQ Rules Passed vs Failed",
                        "metrics": [
                            [NAMESPACE, "GlueDQ_RulesPassed", "Dataset", "patient_records",
                             {"stat": "Sum", "period": 300, "color": "#2ca02c"}],
                            [NAMESPACE, "GlueDQ_RulesFailed", "Dataset", "patient_records",
                             {"stat": "Sum", "period": 300, "color": "#d62728"}]
                        ],
                        "view": "bar",
                        "region": DEMO_REGION,
                    },
                },
                {
                    "type": "metric",
                    "x": 8, "y": 6, "width": 8, "height": 6,
                    "properties": {
                        "title": "Validation Errors & Warnings",
                        "metrics": [
                            [NAMESPACE, "Validation_ErrorCount", "Validator", "clinical-validator",
                             {"stat": "Sum", "period": 300, "color": "#d62728"}],
                            [NAMESPACE, "Validation_WarningCount", "Validator", "clinical-validator",
                             {"stat": "Sum", "period": 300, "color": "#ff7f0e"}]
                        ],
                        "view": "timeSeries",
                        "region": DEMO_REGION,
                    },
                },
                {
                    "type": "metric",
                    "x": 16, "y": 6, "width": 8, "height": 6,
                    "properties": {
                        "title": "Records Processed per Batch",
                        "metrics": [
                            [NAMESPACE, "GlueDQ_TotalRecords", "Dataset", "patient_records",
                             {"stat": "Sum", "period": 300}]
                        ],
                        "view": "singleValue",
                        "region": DEMO_REGION,
                    },
                },
                {
                    "type": "metric",
                    "x": 0, "y": 12, "width": 24, "height": 6,
                    "properties": {
                        "title": "Errors by Type",
                        "metrics": [
                            [NAMESPACE, "Validation_ErrorsByType", "ErrorType", "INVALID_ICD10",
                             "Validator", "clinical-validator", {"stat": "Sum", "period": 300}],
                            [NAMESPACE, "Validation_ErrorsByType", "ErrorType", "VITAL_OUT_OF_RANGE",
                             "Validator", "clinical-validator", {"stat": "Sum", "period": 300}],
                            [NAMESPACE, "Validation_ErrorsByType", "ErrorType", "DRUG_INTERACTION",
                             "Validator", "clinical-validator", {"stat": "Sum", "period": 300}],
                            [NAMESPACE, "Validation_ErrorsByType", "ErrorType", "MISSING_REQUIRED_FIELD",
                             "Validator", "clinical-validator", {"stat": "Sum", "period": 300}],
                        ],
                        "view": "bar",
                        "region": DEMO_REGION,
                    },
                },
            ]
        }),
    }


def publish_metrics_example(metrics: list[dict]) -> dict:
    """Generate the boto3 API call to publish metrics.

    In production, call:
        cloudwatch = boto3.client('cloudwatch')
        cloudwatch.put_metric_data(Namespace=NAMESPACE, MetricData=batch)
    """
    # CloudWatch accepts max 1000 metric data points per call, 150 per batch recommended
    batches = [metrics[i:i+20] for i in range(0, len(metrics), 20)]
    return {
        "api": "cloudwatch.put_metric_data",
        "namespace": NAMESPACE,
        "total_metrics": len(metrics),
        "batches": len(batches),
        "example_call": {
            "Namespace": NAMESPACE,
            "MetricData": metrics[:3],  # Show first 3 as example
        },
        "note": "Use boto3 cloudwatch.put_metric_data() in production. "
                "Metrics are batched in groups of 20 for API efficiency."
    }


def main():
    """Generate CloudWatch configurations from validation results."""
    # Load validation results if available
    glue_report_path = Path(__file__).parent.parent / "glue_quality" / "output" / "glue_dq_report.json"
    validation_path = Path(__file__).parent.parent / "lambda_validation" / "output" / "validation_results.json"

    glue_report = {}
    validation_summary = {}

    if glue_report_path.exists():
        with open(glue_report_path) as f:
            glue_report = json.load(f)
        print(f"Loaded Glue DQ report: score={glue_report.get('overall_score')}%")
    else:
        # Use sample data for standalone testing
        glue_report = {
            "overall_score": 85.7, "rules_passed": 12, "rules_failed": 2,
            "total_records": 200
        }
        print("Using sample Glue DQ data (run glue_dq_rules.py first for real data)")

    if validation_path.exists():
        with open(validation_path) as f:
            validation_summary = json.load(f)
        print(f"Loaded validation summary: {validation_summary.get('passed')}/{validation_summary.get('total')} passed")
    else:
        validation_summary = {
            "total": 200, "passed": 170, "failed": 15, "warnings": 15,
            "errors_by_type": {
                "INVALID_ICD10": 5, "VITAL_OUT_OF_RANGE": 4,
                "DRUG_INTERACTION": 8, "MISSING_REQUIRED_FIELD": 3
            }
        }
        print("Using sample validation data (run clinical_validator.py first for real data)")

    # Build all CloudWatch configurations
    metrics = build_cloudwatch_metric_data(glue_report, validation_summary)
    alarms = build_cloudwatch_alarms()
    dashboard = build_cloudwatch_dashboard()
    publish_example = publish_metrics_example(metrics)

    # Save all configurations
    with open(OUTPUT_DIR / "cloudwatch_metrics.json", "w") as f:
        json.dump(metrics, f, indent=2)

    with open(OUTPUT_DIR / "cloudwatch_alarms.json", "w") as f:
        json.dump(alarms, f, indent=2)

    with open(OUTPUT_DIR / "cloudwatch_dashboard.json", "w") as f:
        json.dump(dashboard, f, indent=2)

    with open(OUTPUT_DIR / "publish_metrics_example.json", "w") as f:
        json.dump(publish_example, f, indent=2)

    print(f"\n{'='*60}")
    print("CLOUDWATCH CONFIGURATION GENERATED")
    print(f"{'='*60}")
    print(f"Metrics defined:    {len(metrics)}")
    print(f"Alarms defined:     {len(alarms)}")
    print(f"Dashboard widgets:  {len(json.loads(dashboard['DashboardBody'])['widgets'])}")
    print(f"\nFiles saved to: {OUTPUT_DIR}")
    print(f"  - cloudwatch_metrics.json")
    print(f"  - cloudwatch_alarms.json")
    print(f"  - cloudwatch_dashboard.json")
    print(f"  - publish_metrics_example.json")
    print(f"\nTo deploy to AWS:")
    print(f"  1. cloudwatch.put_metric_data(Namespace='{NAMESPACE}', MetricData=...)")
    print(f"  2. cloudwatch.put_metric_alarm(**alarm_config) for each alarm")
    print(f"  3. cloudwatch.put_dashboard(**dashboard_config)")


if __name__ == "__main__":
    main()
