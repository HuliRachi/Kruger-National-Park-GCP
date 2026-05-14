from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name, when

# create spark session
spark = SparkSession.builder.appName("Kruger Free Entrance").getOrCreate()

#configure variables
BUCKET_NAME = "kruger-bucket"
FREE_ENTRANCE_BUCKET_PATH = f"gs://{BUCKET_NAME}/landing/free_entrance/*.csv"
BQ_TABLE = "project-a2ce378b-71f9-4087-95b.bronze_dataset_september_free_entry"
TEMP_GCS_BUCKET = f"{BUCKET_NAME}/temp"

#steep 1 read from free-entrance source
free_entrance_df = spark.read.csv(FREE_ENTRANCE_BUCKET_PATH, header=True)

# write to bigquery

free_entrance_df.write.format("bigquery").option("table", BQ_TABLE).option("TemporaryGCBucket", TEMP_GCS_BUCKET).mode("overwrite").save()