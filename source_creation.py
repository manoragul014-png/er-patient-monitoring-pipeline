import pandas as pd
import random
from datetime import datetime, timedelta

# Number of records
num_records = 1000

# Sample doctor names
doctors = [
    "Dr. Smith", "Dr. Johnson", "Dr. Williams", "Dr. Brown",
    "Dr. Jones", "Dr. Garcia", "Dr. Miller", "Dr. Davis"
]

# Sample triage levels
triage_levels = ["Low", "Medium", "High", "Critical"]

# Base date for generating timestamps
start_date = datetime(2025, 1, 1, 0, 0, 0)

data = []

for i in range(1, num_records + 1):
    patient_id = f"P{i:04d}"

    # Random arrival time within 30 days
    arrival_time = start_date + timedelta(
        days=random.randint(0, 29),
        hours=random.randint(0, 23),
        minutes=random.randint(0, 59)
    )

    triage_level = random.choice(triage_levels)
    doctor_assigned = random.choice(doctors)

    # Random patient status
    patient_status = random.choice(["waiting", "under_treatment", "treated"])

    # Set treatment/discharge times based on patient status
    if patient_status == "waiting":
        treatment_start_time = None
        discharge_time = None
    elif patient_status == "under_treatment":
        treatment_start_time = arrival_time + timedelta(minutes=random.randint(5, 120))
        discharge_time = None
    else:  # treated
        treatment_start_time = arrival_time + timedelta(minutes=random.randint(5, 120))
        discharge_time = treatment_start_time + timedelta(minutes=random.randint(30, 480))

    data.append([
        patient_id,
        arrival_time,
        triage_level,
        doctor_assigned,
        treatment_start_time,
        discharge_time,
        patient_status
    ])

# Create DataFrame
df = pd.DataFrame(data, columns=[
    "patient_id",
    "arrival_time",
    "triage_level",
    "doctor_assigned",
    "treatment_start_time",
    "discharge_time",
    "patient_status"
])

# Output file path
output_path = '/Users/manoragul014/Desktop/DE Project/patient_monitoring_data.csv'

# Save to CSV
df.to_csv(output_path, index=False)

print(f"{output_path} created successfully with 1000 records.")
print(df.head())