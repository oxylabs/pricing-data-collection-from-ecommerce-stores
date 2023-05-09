import requests
import glom

from datetime import datetime

from airflow.decorators import task
from airflow import DAG
import sqlite3

from settings import OXY_USERNAME, OXY_PASSWORD, DB_FILE_PATH


@task(task_id="collect_crawl_job")
def collect_crawl_jobs():
    conn = sqlite3.connect(DB_FILE_PATH)
    cursor = conn.cursor()

    cursor.execute(
        f"""
            SELECT job_id FROM crawl_jobs
            WHERE status = 'pending'
        """
    )
    jobs = cursor.fetchall()
    if not jobs:
        print("No pending crawl jobs found")
        return

    job_ids = [_id[0] for _id in jobs]

    print(f"JOB IDS: {job_ids}")

    for job_id in job_ids:
        meta_url = f"http://ect.oxylabs.io/v1/jobs/{job_id}"
        response = requests.get(
            url=meta_url,
            auth=(OXY_USERNAME, OXY_PASSWORD),
        ).json()

        events = response.get("events", [])
        if len(events) == 0:
            print("Crawling job is not finished.")
            return

        indexing_event = events[0]
        if indexing_event.get("status", "") == "faulted":
            cursor.execute(
                f"""
                    UPDATE crawl_jobs 
                    SET status = 'faulted'
                    WHERE job_id = {job_id}
                """
            )
            conn.commit()
            return

        sitemap_url = f"http://ect.oxylabs.io/v1/jobs/{job_id}/sitemap"
        response = requests.get(
            url=sitemap_url,
            auth=(OXY_USERNAME, OXY_PASSWORD),
        ).json()

        products = glom.glom(response, "results.0.sitemap")
        for product_url in products:
            try:
                cursor.execute(
                    f"""
                        INSERT INTO products (url, crawl_job_id)
                        VALUES ('{product_url}', '{job_id}')
                    """
                )
                conn.commit()
                print(f"New product {product_url}")
            except sqlite3.IntegrityError:
                print(f"Already exist {product_url}")

        print(f"Setting crawl job {job_id} to done.")
        cursor.execute(
            f"""
                UPDATE crawl_jobs 
                SET status = 'done'
                WHERE job_id = '{job_id}'
            """
        )
        conn.commit()


with DAG(
    "collect_crawl_job",
    description="Collect results from crawl jobs.",
    schedule_interval="0 7 * * *",
    start_date=datetime(2022, month=6, day=6, hour=13),
    catchup=False,
    tags=["webinar"],
    default_args={
        "owner": "airflow",
    },
) as dag:
    (collect_crawl_jobs())
