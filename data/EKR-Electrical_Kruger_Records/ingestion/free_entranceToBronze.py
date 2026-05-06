from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name, when

# create spark session
spark = SparkSession.builder.appName("Kruger Free Entrance").getOrCreate()

#configure variables
BUCKET_NAME = "bucketname............."
FREE_ENTRANCE_BUCKET_PATH = f"gs://{BUCKET_NAME}/landing/free_entrance/*.csv"
BQ_TABLE = "project.....huli.bronze_dataset_september_free_entry"
TEMP_GCS_BUCKET = f"{BUCKET_NAME}/temp"

#steep 1 read from free-entrance source
free_entrance_df = spark.read.csv(FREE_ENTRANCE_BUCKET_PATH, header=True)

# write to bigquery

free_entrance_df.write.format("bigquery").option("table", BQ_TABLE).option("TemporaryGCBucket", TEMP_GCS_BUCKET).mode("overwrite").save()