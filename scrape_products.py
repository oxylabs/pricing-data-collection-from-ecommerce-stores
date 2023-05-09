import requests
import glom
import sqlite3

from datetime import datetime

from airflow.decorators import task
from airflow import DAG

from settings import OXY_USERNAME, OXY_PASSWORD, DB_FILE_PATH


@task(task_id="get_products")
def get_products():
    conn = sqlite3.connect(DB_FILE_PATH)
    cursor = conn.cursor()

    cursor.execute(
        f"""
            SELECT url FROM products
        """
    )
    urls = [url[0] for url in cursor.fetchall()]
    return urls


@task
def scrape_products(product):
    print(f"Scraping {product}")
    payload = {
        "source": "universal",
        "url": product,
        "parse": True,
        "parsing_instructions": {
            "price": {
                "_fns": [
                    {
                        "_fn": "xpath_one",
                        "_args": [
                            "//p[@class=\"price_color\"]/text()"
                        ]
                    },
                    {
                        "_fn": "amount_from_string"
                    }
                ]
            },
        }
    }
    response = requests.post(
        url="https://realtime.oxylabs.io/v1/queries",
        auth=(OXY_USERNAME, OXY_PASSWORD),
        json=payload
    ).json()

    parsing_status_code = glom.glom(response, "results.0.content.parse_status_code")
    if parsing_status_code == 12000:
        price = glom.glom(response, "results.0.content.price")
        conn = sqlite3.connect(DB_FILE_PATH)
        cursor = conn.cursor()
        cursor.execute(
            f"""
                INSERT INTO product_prices (product_url, price)
                VALUES ('{product}', '{price}')
            """
        )
        conn.commit()
        print(f"Price {price} added for {product}")


with DAG(
    "scrape_products",
    description="Scraper products.",
    schedule_interval="0 7 * * *",
    start_date=datetime(2022, month=6, day=6, hour=13),
    catchup=False,
    tags=["webinar"],
    default_args={
        "owner": "airflow",
    },
) as dag:
    (scrape_products.expand(product=get_products()))
