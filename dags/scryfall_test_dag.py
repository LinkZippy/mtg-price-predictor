from airflow.sdk import dag, task
from datetime import datetime
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import requests
import json

@dag(
    dag_id = "scryfall_test_dag",
    start_date = datetime(2026, 1, 1),
    schedule = None,
    catchup = False,
)

def scryfall_test_dag():

    @task
    def fetch_one_card():
        headers = {
            "User-Agent": "PhoomMTGPricePredictor/0.1",
            "Accept": "application/json",
        }
        response = requests.get("https://api.scryfall.com/cards/random", headers=headers)
        response.raise_for_status()
        card = response.json()
        print(f'Fetched card: {card["name"]}')
        return {
            "name": card["name"],
            "set": card["set"],
            "usd_price": card.get("prices", {}).get("usd"),
        }
    
    @task
    def upload_to_s3(card_info : dict):
        hook = S3Hook(aws_conn_id="aws_default")
        file_content = json.dumps(card_info)

        hook.load_string(
            string_data = file_content,
            key = "test/one_card.json",
            bucket_name = "mtg-price-predictor-phoom",
            replace = True,
        )

        print(f'Uploaded card info to S3: {card_info}')

    upload_to_s3(fetch_one_card())

scryfall_test_dag()