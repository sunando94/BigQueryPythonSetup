import os

import pyspark.sql.functions as F

from Constant import SERVICE_ACCOUNT_JSON, PROJECT_ID, JSON_FILE_NAME
from Utility import getSparkSession, flatten_df, fetchCurrencyRates


def main():
    os.environ["SPARK_HOME"] = "./venv/lib/python3.9/site-packages/pyspark"
    def run_fast_scandir(dir):    # dir: str, ext: list
        subfolders, files = [], []
        for f in os.scandir(dir):
            if f.is_dir():
                subfolders.append(f.path)
                print("dir : "+f.name.lower())
            if f.is_file():
                print("file : "+f.name.lower())

    run_fast_scandir(os.getcwd())
    print("Starting Main")
    print("fetching data from API")
    fetchCurrencyRates()
    print("creating spark session")
    session = getSparkSession()
    print("reading json data dump")
    df = session.read.option("multiline", "true").json(JSON_FILE_NAME)
    df1 = (df.withColumn("tmp", F.arrays_zip("USD", "GBP", "CHF"))
           .withColumn("tmp", F.explode("tmp"))
           .select(F.col("tmp.USD"), F.col("tmp.GBP"), F.col("tmp.CHF"))
           )
    session._jsc.hadoopConfiguration().set('fs.gs.impl', 'com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem')
    # This is required if you are using service account and set true,
    session._jsc.hadoopConfiguration().set('fs.gs.auth.service.account.enable', 'true')
    session._jsc.hadoopConfiguration().set('google.cloud.auth.service.account.json.keyfile', SERVICE_ACCOUNT_JSON)
    # # writing to bigquery
    print("Writting to big query")
    (flatten_df(df1).write.mode("overwrite").format("BigQuery")
     .option("credentialsFile", SERVICE_ACCOUNT_JSON)
     .option("project", PROJECT_ID)
     .option("parentProject", PROJECT_ID)
     .option('table', 'testTable')
     .option("temporaryGcsBucket", "sunando_gcs_bucket")
     .option("dataset", "sunandoDataset").mode("overwrite").save())
    print("Write complete")
    print("reading from big query")
    (session.read.format("BigQuery")
     .option("credentialsFile", SERVICE_ACCOUNT_JSON)
     .option("project", PROJECT_ID)
     .option("parentProject", PROJECT_ID)
     .option('table', 'testTable')
     .option("dataset", "sunandoDataset").load().show())


main()

