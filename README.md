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

## Solution Overview
This project creates an automated cloud ETL workflow that:
1. ingests patient data from a local/on-prem source
2. uploads it to Amazon S3
3. triggers an AWS Glue ETL job
4. validates job execution
5. updates metadata using a Glue Crawler
6. prepares analytics-ready data for querying and monitoring

## Expected Outcome
The pipeline enables:
- better visibility into patient inflow
- improved tracking of waiting times
- structured monitoring of treatment stages
- easier ER operations analysis
- stronger support for decision-making and resource planning