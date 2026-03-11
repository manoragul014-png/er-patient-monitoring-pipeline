import pandas as pd
import random
from datetime import datetime, timedelta

# File path
file_path = '/Users/manoragul014/Desktop/DE Project/er_patient_data_onprem.csv'

# Read existing file
existing_df = pd.read_csv(file_path)

# Number of new rows to add
new_records = 50

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

# Start patient numbering after existing rows
start_index = len(existing_df) + 1

for i in range(start_index, start_index + new_records):
    patient_id = f"P{i:04d}"

    arrival_time = start_date + timedelta(
        days=random.randint(0, 29),
        hours=random.randint(0, 23),
        minutes=random.randint(0, 59)
    )

    triage_level = random.choice(triage_levels)
    doctor_assigned = random.choice(doctors)

    patient_status = random.choice(["waiting", "under_treatment", "treated"])

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

# Create new dataframe
new_df = pd.DataFrame(data, columns=[
    "patient_id",
    "arrival_time",
    "triage_level",
    "doctor_assigned",
    "treatment_start_time",
    "discharge_time",
    "patient_status"
])

# Append new rows
updated_df = pd.concat([existing_df, new_df], ignore_index=True)

# Save back to same CSV
updated_df.to_csv(file_path, index=False)

print(f"50 new rows added successfully.")
print(f"Total rows now: {len(updated_df)}")
print(updated_df.tail())