import azure.functions as func
import os
import logging
import requests
import pandas as pd
from datetime import datetime
from azure.storage.blob import BlobServiceClient

app = func.FunctionApp()

# 🔐 Replace this with your actual connection string
BLOB_CONNECTION_STRING = os.environ["BLOB_CONNECTION_STRING"]
CONTAINER_NAME = "crypto-pulse-con"

@app.schedule(schedule="* * * * *", arg_name="myTimer", run_on_startup=False, use_monitor=True)
def fetch_top_10_cryptos(myTimer: func.TimerRequest) -> None:
    logging.info("⏱️ Timer triggered: Fetching top 10 cryptocurrencies...")

    try:
        # 🔗 Call CoinGecko API
        url = "https://api.coingecko.com/api/v3/coins/markets"
        params = {
            "vs_currency": "usd",
            "order": "market_cap_desc",
            "per_page": 10,
            "page": 1,
            "sparkline": False
        }

        response = requests.get(url, params=params)
        response.raise_for_status()
        coins = response.json()

        # 🧠 Extract relevant fields
        data = []
        timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        for coin in coins:
            data.append({
                "timestamp": timestamp,
                "name": coin["name"],
                "symbol": coin["symbol"],
                "price_usd": coin["current_price"],
                "market_cap_usd": coin["market_cap"],
                "volume_usd": coin["total_volume"],
                "price_change_pct_24h": coin["price_change_percentage_24h"]
            })

        # 📊 Convert to DataFrame
        df = pd.DataFrame(data)

        # 🗃️ Save as CSV to Azure Blob
        filename = f"crypto_prices_{datetime.utcnow().strftime('%Y%m%dT%H%M%S')}.csv"
        csv_data = df.to_csv(index=False)

        blob_service = BlobServiceClient.from_connection_string(BLOB_CONNECTION_STRING)
        blob_client = blob_service.get_blob_client(container=CONTAINER_NAME, blob=filename)
        blob_client.upload_blob(csv_data, overwrite=True)

        logging.info(f"✅ File '{filename}' uploaded to container '{CONTAINER_NAME}' successfully.")

    except Exception as e:
        logging.error(f"❌ Error occurred: {e}")
