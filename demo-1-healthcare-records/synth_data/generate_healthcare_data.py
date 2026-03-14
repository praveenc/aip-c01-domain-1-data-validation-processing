"""Generate realistic synthetic healthcare patient records with intentional quality issues.

Produces:
  - synth_data/output/patient_records.json  (structured records)
  - synth_data/output/patient_records.csv   (tabular format for Glue DQ)
  - synth_data/output/clinical_notes.json   (unstructured text notes)

~10-15% of records contain intentional quality problems for validation testing.
"""

import json
import csv
import random
from datetime import datetime, timedelta
from pathlib import Path

# ---------------------------------------------------------------------------
# Deterministic seed for reproducibility
# ---------------------------------------------------------------------------
random.seed(42)

OUTPUT_DIR = Path(__file__).parent / "output"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# ---------------------------------------------------------------------------
# Reference data
# ---------------------------------------------------------------------------
FIRST_NAMES = ["James", "Mary", "Robert", "Patricia", "John", "Jennifer", "Michael",
               "Linda", "David", "Elizabeth", "William", "Barbara", "Richard", "Susan",
               "Joseph", "Jessica", "Thomas", "Sarah", "Charles", "Karen"]

LAST_NAMES = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller",
              "Davis", "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez",
              "Wilson", "Anderson", "Thomas", "Taylor", "Moore", "Jackson", "Martin"]

# Valid ICD-10 codes with descriptions
ICD10_CODES = {
    "E11.9": "Type 2 diabetes mellitus without complications",
    "I10": "Essential (primary) hypertension",
    "J44.1": "Chronic obstructive pulmonary disease with acute exacerbation",
    "I25.10": "Atherosclerotic heart disease of native coronary artery",
    "N18.3": "Chronic kidney disease, stage 3",
    "J18.9": "Pneumonia, unspecified organism",
    "I50.9": "Heart failure, unspecified",
    "K21.0": "Gastro-esophageal reflux disease with esophagitis",
    "M54.5": "Low back pain",
    "F32.1": "Major depressive disorder, single episode, moderate",
}

# Invalid ICD-10 codes for quality issues
INVALID_ICD10 = ["ZZ99.9", "X00", "ABC.1", "999.99", ""]

MEDICATIONS = [
    {"name": "Metformin", "dose": "500mg", "frequency": "BID"},
    {"name": "Metformin", "dose": "1000mg", "frequency": "BID"},
    {"name": "Lisinopril", "dose": "10mg", "frequency": "QD"},
    {"name": "Lisinopril", "dose": "20mg", "frequency": "QD"},
    {"name": "Atorvastatin", "dose": "40mg", "frequency": "QHS"},
    {"name": "Omeprazole", "dose": "20mg", "frequency": "QD"},
    {"name": "Amlodipine", "dose": "5mg", "frequency": "QD"},
    {"name": "Albuterol", "dose": "90mcg", "frequency": "PRN"},
    {"name": "Sertraline", "dose": "50mg", "frequency": "QD"},
    {"name": "Aspirin", "dose": "81mg", "frequency": "QD"},
    {"name": "Furosemide", "dose": "40mg", "frequency": "BID"},
    {"name": "Warfarin", "dose": "5mg", "frequency": "QD"},
]

CLINICAL_NOTE_TEMPLATES = [
    "Patient presented with {symptom}. Physical exam revealed {finding}. "
    "Assessment: {diagnosis}. Plan: {plan}. Follow-up in {followup} weeks.",

    "Chief complaint: {symptom}. History of present illness: Patient reports "
    "{duration} of {symptom}. Review of systems: {ros}. "
    "Impression: {diagnosis}. Rx: {medication}.",

    "Discharge summary — {patient_name} admitted on {admit_date} for {diagnosis}. "
    "Hospital course: {course}. Condition at discharge: {condition}. "
    "Discharge medications: {med_list}. Follow-up: {followup_instructions}.",
]

SYMPTOMS = ["chest pain", "shortness of breath", "persistent cough",
            "lower back pain", "fatigue and malaise", "abdominal discomfort",
            "dizziness", "bilateral leg edema", "headache", "joint stiffness"]

FINDINGS = ["crackles in bilateral lung bases", "regular rate and rhythm",
            "tenderness in RLQ", "2+ pitting edema bilateral LE",
            "decreased breath sounds", "blood pressure elevated at 162/95",
            "normal neurological exam", "mild hepatomegaly"]

PLANS = ["Start antibiotic therapy", "Adjust insulin regimen", "Cardiac catheterization",
         "Physical therapy referral", "Pulmonary function testing",
         "Renal ultrasound", "Increase diuretic dose", "Psychiatric evaluation"]


def generate_vitals(introduce_error: bool = False) -> dict:
    """Generate realistic vital signs, optionally with out-of-range values."""
    vitals = {
        "systolic_bp": random.randint(100, 160),
        "diastolic_bp": random.randint(60, 100),
        "heart_rate": random.randint(55, 105),
        "temperature_f": round(random.uniform(97.0, 99.5), 1),
        "respiratory_rate": random.randint(12, 22),
        "oxygen_saturation": random.randint(93, 100),
    }
    if introduce_error:
        error_type = random.choice(["impossible_bp", "extreme_temp", "negative_hr", "missing"])
        if error_type == "impossible_bp":
            vitals["systolic_bp"] = random.randint(300, 500)  # Impossible value
        elif error_type == "extreme_temp":
            vitals["temperature_f"] = round(random.uniform(110.0, 120.0), 1)
        elif error_type == "negative_hr":
            vitals["heart_rate"] = -random.randint(1, 50)
        elif error_type == "missing":
            del vitals["systolic_bp"]
            del vitals["diastolic_bp"]
    return vitals


def generate_clinical_note(patient_name: str, diagnosis: str, meds: list) -> str:
    """Generate a realistic clinical note from templates."""
    template = random.choice(CLINICAL_NOTE_TEMPLATES)
    med_names = ", ".join(f"{m['name']} {m['dose']} {m['frequency']}" for m in meds)
    admit_date = (datetime.now() - timedelta(days=random.randint(1, 14))).strftime("%Y-%m-%d")

    note = template.format(
        symptom=random.choice(SYMPTOMS),
        finding=random.choice(FINDINGS),
        diagnosis=diagnosis,
        plan=random.choice(PLANS),
        followup=random.randint(1, 8),
        patient_name=patient_name,
        admit_date=admit_date,
        duration=f"{random.randint(1, 14)} days",
        ros="positive for fatigue, negative for fever",
        medication=med_names,
        course="Uncomplicated. Patient improved with treatment.",
        condition=random.choice(["stable", "improved", "fair"]),
        med_list=med_names,
        followup_instructions=f"PCP in {random.randint(1, 4)} weeks, cardiology in {random.randint(2, 8)} weeks",
    )
    return note


def generate_patient_record(record_id: int, introduce_issues: bool = False) -> dict:
    """Generate a single patient record."""
    first = random.choice(FIRST_NAMES)
    last = random.choice(LAST_NAMES)
    age = random.randint(25, 89)
    gender = random.choice(["M", "F"])
    mrn = f"MRN-{record_id:06d}"

    # Select diagnoses
    num_diagnoses = random.randint(1, 3)
    if introduce_issues and random.random() < 0.5:
        # Mix valid and invalid ICD-10 codes
        codes = random.sample(list(ICD10_CODES.keys()), min(num_diagnoses - 1, len(ICD10_CODES)))
        codes.append(random.choice(INVALID_ICD10))
    else:
        codes = random.sample(list(ICD10_CODES.keys()), min(num_diagnoses, len(ICD10_CODES)))

    diagnoses = [{"icd10": c, "description": ICD10_CODES.get(c, "UNKNOWN")} for c in codes]

    # Select medications
    num_meds = random.randint(1, 5)
    meds = random.sample(MEDICATIONS, min(num_meds, len(MEDICATIONS)))

    # Vitals
    vitals = generate_vitals(introduce_error=introduce_issues and random.random() < 0.4)

    # Admission/discharge dates
    discharge_date = datetime.now() - timedelta(days=random.randint(0, 60))
    los = random.randint(1, 14)
    admission_date = discharge_date - timedelta(days=los)

    record = {
        "record_id": record_id,
        "mrn": mrn,
        "patient_name": f"{first} {last}",
        "age": age,
        "gender": gender,
        "admission_date": admission_date.strftime("%Y-%m-%d"),
        "discharge_date": discharge_date.strftime("%Y-%m-%d"),
        "length_of_stay_days": los,
        "diagnoses": diagnoses,
        "medications": meds,
        "vitals_at_discharge": vitals,
        "attending_physician": f"Dr. {random.choice(LAST_NAMES)}",
        "department": random.choice(["Internal Medicine", "Cardiology", "Pulmonology",
                                      "Endocrinology", "Nephrology", "General Surgery"]),
    }

    # Introduce additional quality issues
    if introduce_issues:
        issue_type = random.choice(["missing_name", "future_date", "negative_age",
                                     "empty_meds", "duplicate_mrn", "encoding"])
        if issue_type == "missing_name":
            record["patient_name"] = ""
        elif issue_type == "future_date":
            record["discharge_date"] = (datetime.now() + timedelta(days=30)).strftime("%Y-%m-%d")
        elif issue_type == "negative_age":
            record["age"] = -5
        elif issue_type == "empty_meds":
            record["medications"] = []
        elif issue_type == "encoding":
            record["patient_name"] = "Ren\xe9 Müller-Straße"  # Non-ASCII
            record["department"] = "Cardiología"

    return record


def main():
    num_records = 200
    error_rate = 0.12  # 12% of records will have intentional issues

    records = []
    clinical_notes = []

    for i in range(1, num_records + 1):
        has_issues = random.random() < error_rate
        record = generate_patient_record(i, introduce_issues=has_issues)
        records.append(record)

        # Generate corresponding clinical note
        primary_dx = record["diagnoses"][0]["description"] if record["diagnoses"] else "Unknown"
        note = generate_clinical_note(
            record["patient_name"], primary_dx, record["medications"]
        )
        clinical_notes.append({
            "record_id": record["record_id"],
            "mrn": record["mrn"],
            "note_type": random.choice(["Discharge Summary", "Progress Note", "H&P"]),
            "note_text": note,
            "author": record["attending_physician"],
            "date": record["discharge_date"],
        })

    # Write JSON
    with open(OUTPUT_DIR / "patient_records.json", "w") as f:
        json.dump(records, f, indent=2)

    # Write CSV (flattened for Glue DQ)
    csv_fields = ["record_id", "mrn", "patient_name", "age", "gender",
                  "admission_date", "discharge_date", "length_of_stay_days",
                  "primary_icd10", "primary_diagnosis", "num_medications",
                  "systolic_bp", "diastolic_bp", "heart_rate", "temperature_f",
                  "department", "attending_physician"]

    with open(OUTPUT_DIR / "patient_records.csv", "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=csv_fields)
        writer.writeheader()
        for r in records:
            vitals = r.get("vitals_at_discharge", {})
            flat = {
                "record_id": r["record_id"],
                "mrn": r["mrn"],
                "patient_name": r["patient_name"],
                "age": r["age"],
                "gender": r["gender"],
                "admission_date": r["admission_date"],
                "discharge_date": r["discharge_date"],
                "length_of_stay_days": r["length_of_stay_days"],
                "primary_icd10": r["diagnoses"][0]["icd10"] if r["diagnoses"] else "",
                "primary_diagnosis": r["diagnoses"][0]["description"] if r["diagnoses"] else "",
                "num_medications": len(r["medications"]),
                "systolic_bp": vitals.get("systolic_bp", ""),
                "diastolic_bp": vitals.get("diastolic_bp", ""),
                "heart_rate": vitals.get("heart_rate", ""),
                "temperature_f": vitals.get("temperature_f", ""),
                "department": r["department"],
                "attending_physician": r["attending_physician"],
            }
            writer.writerow(flat)

    # Write clinical notes
    with open(OUTPUT_DIR / "clinical_notes.json", "w") as f:
        json.dump(clinical_notes, f, indent=2)

    # Print summary
    issue_count = sum(1 for r in records if r["age"] < 0 or r["patient_name"] == ""
                      or any(d["icd10"] in INVALID_ICD10 for d in r["diagnoses"]))
    print(f"Generated {len(records)} patient records → {OUTPUT_DIR}")
    print(f"  - patient_records.json ({len(records)} records)")
    print(f"  - patient_records.csv  (flattened for Glue DQ)")
    print(f"  - clinical_notes.json  ({len(clinical_notes)} notes)")
    print(f"  - Records with detectable issues: ~{issue_count}")


if __name__ == "__main__":
    main()
