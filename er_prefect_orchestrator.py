from prefect import flow, task, get_run_logger
import boto3
import time
from botocore.exceptions import ClientError


@task
def upload_local_file_to_s3(local_file: str, bucket: str, s3_key: str):
    logger = get_run_logger()
    s3 = boto3.client("s3")
    s3.upload_file(local_file, bucket, s3_key)
    logger.info(f"Uploaded {local_file} to s3://{bucket}/{s3_key}")


@task
def validate_glue_job(job_name: str, region_name: str = "eu-north-1") -> None:
    logger = get_run_logger()
    glue = boto3.client("glue", region_name=region_name)

    try:
        response = glue.get_job(JobName=job_name)
        logger.info(f"Validated Glue job exists: {response['Job']['Name']}")
    except ClientError as e:
        raise RuntimeError(f"Glue job validation failed: {e}")


@task
def start_glue_job(job_name: str, region_name: str = "eu-north-1") -> str:
    logger = get_run_logger()
    glue = boto3.client("glue", region_name=region_name)

    try:
        response = glue.start_job_run(JobName=job_name)
        run_id = response["JobRunId"]
        logger.info(f"Started Glue job '{job_name}' with run id: {run_id}")
        return run_id
    except ClientError as e:
        raise RuntimeError(f"Failed to start Glue job: {e}")


@task
def wait_for_glue_job(
    job_name: str,
    run_id: str,
    region_name: str = "eu-north-1",
    poll_interval: int = 30
) -> str:
    logger = get_run_logger()
    glue = boto3.client("glue", region_name=region_name)

    terminal_states = {"SUCCEEDED", "FAILED", "STOPPED", "TIMEOUT"}

    while True:
        response = glue.get_job_run(JobName=job_name, RunId=run_id)
        state = response["JobRun"]["JobRunState"]
        logger.info(f"Glue job '{job_name}' run '{run_id}' status: {state}")

        if state in terminal_states:
            return state

        time.sleep(poll_interval)


@task
def check_glue_job_succeeded(job_name: str, run_id: str, final_state: str) -> None:
    logger = get_run_logger()

    if final_state != "SUCCEEDED":
        raise RuntimeError(
            f"Glue job '{job_name}' with run id '{run_id}' did not succeed. Final state: {final_state}"
        )

    logger.info(f"Glue job '{job_name}' completed successfully.")


@task
def validate_crawler(crawler_name: str, region_name: str = "eu-north-1") -> None:
    logger = get_run_logger()
    glue = boto3.client("glue", region_name=region_name)

    try:
        response = glue.get_crawler(Name=crawler_name)
        logger.info(f"Validated crawler exists: {response['Crawler']['Name']}")
    except ClientError as e:
        raise RuntimeError(f"Crawler validation failed: {e}")


@task
def start_crawler(crawler_name: str, region_name: str = "eu-north-1") -> None:
    logger = get_run_logger()
    glue = boto3.client("glue", region_name=region_name)

    try:
        crawler = glue.get_crawler(Name=crawler_name)
        state = crawler["Crawler"]["State"]

        if state == "RUNNING":
            logger.info(f"Crawler '{crawler_name}' is already running. Skipping start.")
            return

        glue.start_crawler(Name=crawler_name)
        logger.info(f"Started crawler '{crawler_name}'")
    except ClientError as e:
        raise RuntimeError(f"Failed to start crawler: {e}")


@task
def wait_for_crawler_completion(
    crawler_name: str,
    region_name: str = "eu-north-1",
    poll_interval: int = 30
) -> None:
    logger = get_run_logger()
    glue = boto3.client("glue", region_name=region_name)

    while True:
        response = glue.get_crawler(Name=crawler_name)
        state = response["Crawler"]["State"]
        last_status = response["Crawler"].get("LastCrawl", {}).get("Status", "UNKNOWN")

        logger.info(f"Crawler '{crawler_name}' state: {state}, last crawl status: {last_status}")

        if state == "READY":
            if last_status in {"SUCCEEDED", "CANCELLED", "FAILED"}:
                if last_status != "SUCCEEDED":
                    raise RuntimeError(
                        f"Crawler '{crawler_name}' finished but crawl status is {last_status}"
                    )
                logger.info(f"Crawler '{crawler_name}' completed successfully.")
                return

        time.sleep(poll_interval)


@flow(name="er-patient-monitoring-orchestrator")
def er_patient_monitoring_orchestrator():
    local_file = "/Users/manoragul014/Desktop/DE Project/er_patient_data_onprem.csv"
    bucket = "er-monitoring-bucket"
    s3_key = "er_raw/onprem/input/er_patient_data_onprem.csv"

    glue_job_name = "er_patient_monitoring_job"
    crawler_name = "processed_er_crawler"
    region_name = "eu-north-1"

    upload_local_file_to_s3(local_file, bucket, s3_key)
    validate_glue_job(glue_job_name, region_name)
    run_id = start_glue_job(glue_job_name, region_name)
    final_state = wait_for_glue_job(glue_job_name, run_id, region_name)
    check_glue_job_succeeded(glue_job_name, run_id, final_state)
    validate_crawler(crawler_name, region_name)
    start_crawler(crawler_name, region_name)
    wait_for_crawler_completion(crawler_name, region_name)


if __name__ == "__main__":
    er_patient_monitoring_orchestrator()