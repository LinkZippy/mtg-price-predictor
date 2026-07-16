from airflow.sdk import task, dag
from datetime import datetime
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import requests

HEADERS = {
    "User-Agent": "PhoomMTGPricePredictor/0.1",
    "Accept": "application/json",
}

@dag(
    dag_id = "scryfall_ingest_dag",
    start_date = datetime(2026, 1, 1),
    schedule = "@daily",
    catchup = False,
)

def scryfall_ingest_dag():

    @task
    def get_bulk_data():
        response = requests.get("https://api.scryfall.com/bulk-data", headers=HEADERS)
        response.raise_for_status
        bulk_data_list = response.json()["data"]

        all_cards_entry = next(
            item for item in bulk_data_list if item["type"] == "all_cards"
        )
        download_url = all_cards_entry["download_uri"]
        print(f"all_cards files live at: {download_url}")
        return download_url

    @task
    def download_and_upload(download_url : str, logical_date : datetime = None):
        response = requests.get(download_url, headers = HEADERS, stream = True)
        response.raise_for_status

        response.raw.decode_content = True

        run_date = logical_date.strftime("%Y-%m-%d")
        s3_key = f"raw/scryfall/dt={run_date}/all_cards.json"

        hook = S3Hook(aws_conn_id = "aws_default")
        hook.load_file_obj(
            file_obj = response.raw,
            key = s3_key,
            bucket_name = "mtg-price-predictor-phoom",
            replace = True,
        )

        print(f"Uploaded bulk data to s3://mtg-price-predictor-phoom/{s3_key}")

    download_and_upload(get_bulk_data())

scryfall_ingest_dag()

