import json
import os

import requests
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from requests.auth import HTTPBasicAuth

from Constant import REQUEST_URL, USERNAME, PASSWORD, PARAM, JSON_FILE_NAME


def getSparkSession():
    session = (SparkSession.builder
               .config('spark.executor.userClassPathFirst', True)
               .config('spark.driver.userClassPathFirst', True)
               .config('spark.jars.packages',
                       'com.google.guava:guava:20.0,com.google.cloud.bigdataoss:gcs-connector:hadoop2-2.2.15')
               .config("spark.driver.extraClassPath", "guava-23.1.0-jre.jar:spark-3.5-bigquery-0.37.0.jar")
               .config("spark.executor.extraClassPath", "guava-23.1.0-jre.jar:spark-3.5-bigquery-0.37.0.jar")

               .master(
        'local').appName(
        "SonovaApp").getOrCreate())
    return session


def flatten_df(nested_df):
    stack = [((), nested_df)]
    columns = []
    while len(stack) > 0:
        parents, df_t = stack.pop()
        for column_name, column_type in df_t.dtypes:
            if column_type[:6] == "struct":
                projected_df = df_t.select(column_name + ".*")
                stack.append((parents + (column_name,), projected_df))
            else:
                columns.append(F.col(".".join(parents + (column_name,))).alias("_".join(parents + (column_name,))))
    return nested_df.select(columns)


def fetchCurrencyRates():
    global response, data, f
    if not os.path.exists(JSON_FILE_NAME):
        print(f"Making HTTP request as {JSON_FILE_NAME} not found")
        response = requests.get(url=REQUEST_URL,
                                auth=HTTPBasicAuth(USERNAME, PASSWORD),
                                params=PARAM)

        if response.status_code == 200:
            print(f"response received with status code {response.status_code}")
            data = json.loads(response.text)
            with open(JSON_FILE_NAME, 'w', encoding='utf-8') as f:
                json.dump(data['to'], f, ensure_ascii=False, indent=4)
