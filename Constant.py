SERVICE_ACCOUNT_JSON = "/Users/sunando/Developer/Interview/Sonova_digital/api-project-116381815678-d3345db375a6.json"
REQUEST_URL = 'https://xecdapi.xe.com/v1/historic_rate/period'
PARAM = {"from": "EUR", "to": "USD,GBP,CHF", "start_timestamp": "2024-04-01T00:00"}
USERNAME = 'databricks245518079'
PASSWORD = 'l5pi3fs3t2aa585phpjj8rha5e'
PROJECT_ID = "api-project-116381815678"

import datetime
date = datetime.date.today().strftime("%Y_%m_%d")
JSON_FILE_NAME = f"data_{date}.json"