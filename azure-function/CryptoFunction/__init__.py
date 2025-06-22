import datetime
import logging
import requests 

import azure.functions as func


def get_crypto_price(symbol='bitcoin'):
    url = f"https://api.coingecko.com/api/v3/simple/price?ids={symbol}&vs_currencies=usd"
    response = requests.get(url)
    return response.json()


def main(mytimer: func.TimerRequest) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc).isoformat()

    if mytimer.past_due:
        logging.info('The timer is past due!')

    price_data = get_crypto_price()
    logging.info(f"Fetched crypto data: {price_data} at {utc_timestamp}")

    # (Optional) Save to Blob or database next...
