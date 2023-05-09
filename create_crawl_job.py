import requests

from datetime import datetime

from airflow.decorators import task
from airflow import DAG
import sqlite3

from settings import OXY_USERNAME, OXY_PASSWORD, DB_FILE_PATH


OXYLABS_CRAWLER_PAYLOAD = {
    "url": "https://books.toscrape.com",
    "filters": {
        "crawl": [
            "page-\\d+\\.html"
        ],
        "process": [
            "https://books\\.toscrape\\.com/catalogue/((?!category).)*\\d+/index\\.html"
        ],
        "max_depth": 1
    },
    "output": {
        "type_": "sitemap",
        "aggregate_chunk_size_bytes": 1073741824
    },
}


@task(task_id="create_crawl_job")
def create_crawl_job():
    response = requests.post(
        url="https://ect.oxylabs.io/v1/jobs",
        auth=(OXY_USERNAME, OXY_PASSWORD),
        json=OXYLABS_CRAWLER_PAYLOAD
    )
    job_id = response.json().get("id")

    conn = sqlite3.connect(DB_FILE_PATH)
    cursor = conn.cursor()
    cursor.execute(
        f"""
            INSERT INTO crawl_jobs (job_id, status) 
            VALUES ('{job_id}', 'pending');
        """
    )
    conn.commit()


with DAG(
    "create_crawl_job",
    description="Crawl target website.",
    schedule_interval="0 7 * * *",
    start_date=datetime(2022, month=6, day=6, hour=13),
    catchup=False,
    tags=["webinar"],
    default_args={"owner": "airflow"},
) as dag:
    (create_crawl_job())
