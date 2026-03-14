# AIP-C01 Domain 1: Data Validation & Processing

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![Python 3.9+](https://img.shields.io/badge/Python-3.9%2B-blue.svg)](https://www.python.org/downloads/)
[![AWS Services](https://img.shields.io/badge/AWS-Glue%20%7C%20Lambda%20%7C%20Bedrock%20%7C%20Comprehend%20%7C%20CloudWatch-orange.svg)](https://aws.amazon.com/)
[![AIP-C01](https://img.shields.io/badge/Cert-AWS%20AIP--C01-232F3E?logo=amazon-aws)](https://aws.amazon.com/certification/certified-ai-practitioner/)

Hands-on demos for **AWS Certified AI Practitioner (AIP-C01)** — Domain 1: Fundamentals of AI and ML, focusing on **data validation, quality monitoring, and Bedrock payload formatting** for foundation model pipelines.

## Overview

This repository contains end-to-end demos that cover the data preparation and validation stages of an AI/ML pipeline on AWS. Each demo walks through a realistic scenario — from synthetic data generation through quality checks to Bedrock-ready payload construction — using only standard-library Python (no AWS credentials required for local execution).

### Key Topics Covered

| AIP-C01 Section | Topic |
|-----------------|-------|
| 1.1 | AWS Glue Data Quality — DQDL rules for completeness, consistency, accuracy, and format |
| 1.3 | Custom Lambda validation — domain-specific business logic |
| 1.4 | CloudWatch monitoring — metrics, alarms, and dashboards for data quality |
| 1.5 | Structured (CSV/JSON) and unstructured (free-text) data validation |
| 3.1 | Bedrock `invoke_model` — properly formatted JSON payloads for Claude 3 |
| 4.2 | Amazon Comprehend Medical — entity extraction and standardization |

## Demos

| Demo | Description |
|------|-------------|
| [Demo 1: Healthcare Records](demo-1-healthcare-records/) | Process patient discharge summaries through Glue DQ, Lambda validation, CloudWatch monitoring, Comprehend entity extraction, and Bedrock payload formatting |

## Project Structure

```
.
├── README.md
├── LICENSE
├── .gitignore
├── shared/
│   └── utils/
│       └── bedrock_helpers.py          # Shared utility for Bedrock payload construction
└── demo-1-healthcare-records/
    ├── README.md                       # Detailed walkthrough and architecture
    ├── GLUE-DQ-EXPLAINED.md            # Deep-dive on DQDL rules
    ├── synth_data/                     # Step 1: Synthetic data generation
    │   └── generate_healthcare_data.py
    ├── glue_quality/                   # Step 2: Glue Data Quality rules
    │   └── glue_dq_rules.py
    ├── lambda_validation/              # Step 3: Lambda domain validation
    │   └── clinical_validator.py
    ├── cloudwatch/                     # Step 4: CloudWatch monitoring
    │   └── quality_dashboard.py
    ├── bedrock_formatting/             # Step 5: Comprehend + Bedrock formatting
    │   └── format_for_bedrock.py
    └── scripts/                        # Infrastructure setup/teardown
        ├── setup-aws-infra.sh
        └── teardown-aws-infra.sh
```

## Quick Start

```bash
# Clone the repo
git clone https://github.com/praveenc/aip-c01-domain-1-data-validation-processing.git
cd aip-c01-domain-1-data-validation-processing

# Run the healthcare records demo (all steps)
cd demo-1-healthcare-records/
python synth_data/generate_healthcare_data.py
python glue_quality/glue_dq_rules.py
python lambda_validation/clinical_validator.py
python cloudwatch/quality_dashboard.py
python bedrock_formatting/format_for_bedrock.py
```

> **No AWS credentials or third-party packages required.** All scripts run offline with Python 3.9+ standard library only. AWS API calls are represented as generated configuration files.

## Requirements

- **Python 3.9+** (standard library only — no `pip install` needed)
- No AWS account required for local execution
- For production deployment: AWS account with Glue, Lambda, Bedrock, Comprehend Medical, and CloudWatch access

## License

This project is licensed under the [MIT License](LICENSE).
