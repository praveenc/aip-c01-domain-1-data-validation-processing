"""Comprehend entity extraction and Bedrock payload formatting for healthcare records.

Demonstrates:
  - Amazon Comprehend Medical entity extraction (DetectEntitiesV2)
  - Entity standardization and normalization
  - Constructing properly formatted Bedrock Claude 3 Messages API payloads
  - Parameter tuning (temperature, top_p, max_tokens) for clinical use cases

Covers requirement 3.1: JSON payloads for Bedrock API requests
Covers requirement 4.2: Comprehend entity extraction and standardization
"""

import json
import re
import sys
from pathlib import Path

OUTPUT_DIR = Path(__file__).parent / "output"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Add shared utils to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from shared.utils.bedrock_helpers import build_bedrock_messages_payload


# ---------------------------------------------------------------------------
# Comprehend Medical entity extraction (simulated for local testing)
# ---------------------------------------------------------------------------
def simulate_comprehend_medical(text: str) -> dict:
    """Simulate Amazon Comprehend Medical DetectEntitiesV2 response.

    In production, use:
        client = boto3.client('comprehendmedical')
        response = client.detect_entities_v2(Text=text)

    The actual API returns entities with categories:
      MEDICATION, MEDICAL_CONDITION, TEST_TREATMENT_PROCEDURE,
      ANATOMY, TIME_EXPRESSION, PROTECTED_HEALTH_INFORMATION
    """
    # Simple keyword-based simulation for offline testing
    entities = []
    entity_id = 0

    # Medication patterns
    medications = ["Metformin", "Lisinopril", "Atorvastatin", "Omeprazole",
                   "Amlodipine", "Albuterol", "Sertraline", "Aspirin",
                   "Furosemide", "Warfarin"]
    for med in medications:
        if med.lower() in text.lower():
            idx = text.lower().find(med.lower())
            entities.append({
                "Id": entity_id,
                "Text": text[idx:idx+len(med)],
                "Category": "MEDICATION",
                "Type": "GENERIC_NAME",
                "Score": 0.95,
                "BeginOffset": idx,
                "EndOffset": idx + len(med),
                "Traits": [],
            })
            entity_id += 1

    # Condition patterns
    conditions = {
        "diabetes": "Type 2 diabetes mellitus",
        "hypertension": "Essential hypertension",
        "pneumonia": "Pneumonia",
        "heart failure": "Heart failure",
        "copd": "Chronic obstructive pulmonary disease",
        "chest pain": "Chest pain",
        "shortness of breath": "Dyspnea",
        "back pain": "Low back pain",
    }
    for keyword, normalized in conditions.items():
        if keyword in text.lower():
            idx = text.lower().find(keyword)
            entities.append({
                "Id": entity_id,
                "Text": text[idx:idx+len(keyword)],
                "Category": "MEDICAL_CONDITION",
                "Type": "DX_NAME",
                "Score": 0.92,
                "BeginOffset": idx,
                "EndOffset": idx + len(keyword),
                "Traits": [{"Name": "DIAGNOSIS", "Score": 0.88}],
                "NormalizedText": normalized,
            })
            entity_id += 1

    # PHI patterns (dates, names)
    date_pattern = re.compile(r"\d{4}-\d{2}-\d{2}")
    for match in date_pattern.finditer(text):
        entities.append({
            "Id": entity_id,
            "Text": match.group(),
            "Category": "PROTECTED_HEALTH_INFORMATION",
            "Type": "DATE",
            "Score": 0.99,
            "BeginOffset": match.start(),
            "EndOffset": match.end(),
        })
        entity_id += 1

    return {
        "Entities": entities,
        "UnmappedAttributes": [],
        "ModelVersion": "2.0.0 (simulated)",
    }


def extract_and_standardize_entities(clinical_note: str) -> dict:
    """Extract entities from clinical text and standardize them.

    Returns structured entity data ready for Bedrock prompt construction.
    """
    comprehend_result = simulate_comprehend_medical(clinical_note)

    standardized = {
        "medications": [],
        "conditions": [],
        "dates": [],
        "procedures": [],
        "anatomy": [],
        "phi_detected": False,
    }

    for entity in comprehend_result["Entities"]:
        entry = {
            "text": entity["Text"],
            "confidence": entity["Score"],
            "type": entity.get("Type", ""),
        }

        if entity["Category"] == "MEDICATION":
            standardized["medications"].append(entry)
        elif entity["Category"] == "MEDICAL_CONDITION":
            entry["normalized"] = entity.get("NormalizedText", entity["Text"])
            standardized["conditions"].append(entry)
        elif entity["Category"] == "PROTECTED_HEALTH_INFORMATION":
            standardized["phi_detected"] = True
            standardized["dates"].append(entry)
        elif entity["Category"] == "TEST_TREATMENT_PROCEDURE":
            standardized["procedures"].append(entry)
        elif entity["Category"] == "ANATOMY":
            standardized["anatomy"].append(entry)

    return standardized


# ---------------------------------------------------------------------------
# Bedrock payload construction
# ---------------------------------------------------------------------------
CLINICAL_SYSTEM_PROMPT = """You are a clinical decision support assistant for healthcare professionals. 
Your role is to analyze patient records and provide evidence-based clinical summaries.

Guidelines:
- Use standard medical terminology
- Reference ICD-10 codes when applicable
- Flag potential drug interactions or contraindications
- Provide structured output with clear sections
- Never provide definitive diagnoses — always frame as "clinical considerations"
- Include relevant vital sign trends in your analysis

Output Format:
1. Patient Summary
2. Active Problems List
3. Medication Review & Interaction Alerts
4. Clinical Considerations
5. Recommended Follow-up Actions"""


def format_patient_for_bedrock(record: dict, entities: dict,
                                clinical_note: str = "") -> dict:
    """Format a validated patient record into a Bedrock-ready payload.

    Combines structured data, extracted entities, and clinical notes
    into an optimized prompt for Claude 3 on Bedrock.
    """
    # Build structured patient context
    vitals = record.get("vitals_at_discharge", {})
    vitals_str = ", ".join(f"{k}: {v}" for k, v in vitals.items()) if vitals else "Not recorded"

    diagnoses_str = "\n".join(
        f"  - {d['description']} (ICD-10: {d['icd10']})"
        for d in record.get("diagnoses", [])
    )

    meds_str = "\n".join(
        f"  - {m['name']} {m['dose']} {m['frequency']}"
        for m in record.get("medications", [])
    )

    # Build entity-enriched context
    entity_context = ""
    if entities.get("conditions"):
        conditions = [f"{c['normalized']} (confidence: {c['confidence']:.0%})"
                     for c in entities["conditions"]]
        entity_context += f"\nNLP-Extracted Conditions: {', '.join(conditions)}"
    if entities.get("medications"):
        meds = [m["text"] for m in entities["medications"]]
        entity_context += f"\nNLP-Extracted Medications: {', '.join(meds)}"

    # Construct the user message
    user_message = f"""Patient Record Analysis Request
================================
Patient: {record.get('patient_name', 'Unknown')}, Age: {record.get('age', 'N/A')}, Gender: {record.get('gender', 'N/A')}
MRN: {record.get('mrn', 'N/A')}
Department: {record.get('department', 'N/A')}
Attending: {record.get('attending_physician', 'N/A')}

Admission: {record.get('admission_date', 'N/A')} → Discharge: {record.get('discharge_date', 'N/A')}
Length of Stay: {record.get('length_of_stay_days', 'N/A')} days

Diagnoses:
{diagnoses_str}

Medications:
{meds_str}

Vitals at Discharge: {vitals_str}
{entity_context}

Clinical Note:
{clinical_note if clinical_note else 'No clinical note available.'}

Please provide a comprehensive clinical summary with medication review and follow-up recommendations."""

    # Build Bedrock payload with appropriate parameters for clinical use
    # Low temperature (0.1) for factual clinical output
    # High top_p (0.9) for comprehensive but focused responses
    payload = build_bedrock_messages_payload(
        messages=[{"role": "user", "content": user_message}],
        system=CLINICAL_SYSTEM_PROMPT,
        model_id="anthropic.claude-3-sonnet-20240229-v1:0",
        max_tokens=2048,
        temperature=0.1,  # Low for clinical accuracy
        top_p=0.9,
    )

    return payload


def generate_comprehend_api_example() -> dict:
    """Generate the boto3 API call structure for Comprehend Medical."""
    return {
        "detect_entities": {
            "api": "comprehendmedical.detect_entities_v2",
            "parameters": {
                "Text": "<clinical_note_text>"
            },
            "response_structure": {
                "Entities": [
                    {
                        "Id": 0,
                        "Text": "Metformin",
                        "Category": "MEDICATION",
                        "Type": "GENERIC_NAME",
                        "Score": 0.95,
                        "Traits": [],
                        "Attributes": [
                            {"Type": "DOSAGE", "Text": "500mg", "Score": 0.92},
                            {"Type": "FREQUENCY", "Text": "BID", "Score": 0.88}
                        ]
                    }
                ]
            }
        },
        "detect_phi": {
            "api": "comprehendmedical.detect_phi",
            "parameters": {"Text": "<clinical_note_text>"},
            "note": "Use DetectPHI to identify and optionally redact Protected Health Information"
        },
        "infer_icd10": {
            "api": "comprehendmedical.infer_icd10_cm",
            "parameters": {"Text": "<clinical_note_text>"},
            "note": "Maps clinical text to ICD-10-CM codes automatically"
        }
    }


def main():
    """Process validated records through entity extraction and format for Bedrock."""
    validated_path = (Path(__file__).parent.parent / "lambda_validation"
                      / "output" / "validated_records.json")
    notes_path = (Path(__file__).parent.parent / "synth_data"
                  / "output" / "clinical_notes.json")

    # Load data
    if validated_path.exists():
        with open(validated_path) as f:
            records = json.load(f)
        print(f"Loaded {len(records)} validated records")
    else:
        # Fallback: load raw records
        raw_path = Path(__file__).parent.parent / "synth_data" / "output" / "patient_records.json"
        if not raw_path.exists():
            print("ERROR: No data found. Run generate_healthcare_data.py first.")
            return
        with open(raw_path) as f:
            records = json.load(f)
        print(f"Loaded {len(records)} raw records (run clinical_validator.py for validated data)")

    notes_by_id = {}
    if notes_path.exists():
        with open(notes_path) as f:
            notes = json.load(f)
        notes_by_id = {n["record_id"]: n["note_text"] for n in notes}

    # Process records
    bedrock_payloads = []
    entity_results = []

    for record in records[:20]:  # Process first 20 for demo
        record_id = record.get("record_id")
        clinical_note = notes_by_id.get(record_id, "")

        # Step 1: Extract entities from clinical note
        entities = extract_and_standardize_entities(clinical_note)
        entity_results.append({
            "record_id": record_id,
            "entities": entities,
        })

        # Step 2: Format for Bedrock
        payload = format_patient_for_bedrock(record, entities, clinical_note)
        bedrock_payloads.append({
            "record_id": record_id,
            "payload": payload,
        })

    # Save outputs
    with open(OUTPUT_DIR / "entity_extraction_results.json", "w") as f:
        json.dump(entity_results, f, indent=2)

    with open(OUTPUT_DIR / "bedrock_payloads.json", "w") as f:
        json.dump(bedrock_payloads, f, indent=2)

    # Save a single pretty-printed example
    if bedrock_payloads:
        example = bedrock_payloads[0]
        with open(OUTPUT_DIR / "bedrock_payload_example.json", "w") as f:
            json.dump(example, f, indent=2)

    # Save Comprehend API reference
    with open(OUTPUT_DIR / "comprehend_api_reference.json", "w") as f:
        json.dump(generate_comprehend_api_example(), f, indent=2)

    print(f"\n{'='*60}")
    print("ENTITY EXTRACTION & BEDROCK FORMATTING COMPLETE")
    print(f"{'='*60}")
    print(f"Records processed:          {len(bedrock_payloads)}")
    total_entities = sum(
        len(e["entities"].get("medications", [])) + len(e["entities"].get("conditions", []))
        for e in entity_results
    )
    print(f"Total entities extracted:    {total_entities}")
    phi_count = sum(1 for e in entity_results if e["entities"].get("phi_detected"))
    print(f"Records with PHI detected:   {phi_count}")
    print(f"\nFiles saved to: {OUTPUT_DIR}")
    print(f"  - entity_extraction_results.json")
    print(f"  - bedrock_payloads.json ({len(bedrock_payloads)} payloads)")
    print(f"  - bedrock_payload_example.json (single example)")
    print(f"  - comprehend_api_reference.json")

    # Print example payload summary
    if bedrock_payloads:
        p = bedrock_payloads[0]["payload"]
        body = json.loads(p["body"])
        msg_preview = body["messages"][0]["content"][:200]
        print(f"\n--- Example Payload (Record {bedrock_payloads[0]['record_id']}) ---")
        print(f"Model: {p['modelId']}")
        print(f"Temperature: {body['temperature']}, Top-P: {body['top_p']}, Max Tokens: {body['max_tokens']}")
        print(f"System prompt: {body.get('system', '')[:80]}...")
        print(f"User message preview: {msg_preview}...")


if __name__ == "__main__":
    main()
