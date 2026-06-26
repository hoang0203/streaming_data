import os
import urllib.request
import json
from dotenv import load_dotenv

# Tải các biến môi trường từ file .env
load_dotenv()

url = os.getenv("DEBEZIUM_API_URL")
payload = {
    "name": "orders-connector-v3",
    "config": {
        "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
        "database.hostname": "postgres",
        "database.port": "5432",
        "database.user": os.getenv("POSTGRES_USERNAME"),
        "database.password": os.getenv("POSTGRES_PASSWORD"),
        "database.dbname": os.getenv("POSTGRES_DB_NAME"),
        "topic.prefix": "pgstream_v3",
        "table.include.list": "public.orders",
        "decimal.handling.mode": "string",
        "plugin.name": "pgoutput",
        "slot.name": "debezium_slot_v3",
        "publication.name": "dbz_publication_v3"
    }
}

req = urllib.request.Request(url, data=json.dumps(payload).encode('utf-8'), 
                             headers={'Content-Type': 'application/json'})
try:
    response = urllib.request.urlopen(req)
    print("Connector registered successfully!")
    print(response.read().decode('utf-8'))
except Exception as e:
    print("Error:", e)
    