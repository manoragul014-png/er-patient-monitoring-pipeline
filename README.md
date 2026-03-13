## Business Problem
Emergency departments need near real-time visibility into patient flow, waiting time, triage priority, treatment progress, and discharge status. Without a connected data pipeline, monitoring these activities becomes slow and manual, leading to delays, poor visibility, and inefficient decision-making.

## Business Need
The business needs an automated pipeline to track ER patient data and provide structured insights on:
- patient inflow
- triage urgency
- waiting time before treatment
- treatment progress
- operational reporting and analytics

## Project Objective
Build an **ER Patient Monitoring Pipeline** that ingests patient data, transforms it through an ETL workflow, and stores it in a structured cloud format for analytics. The solution uses **Amazon S3**, **AWS Glue**, **Glue Crawler**, and **Prefect** for orchestration.

## Data Architecture
The architecture (Data flow) used in this project uses different Open source and cloud components as described below:
![Workflow Diagram](work_flow_diag.png)
