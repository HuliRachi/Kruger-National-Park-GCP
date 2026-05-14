from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Gate codes ingestion").getOrCreate()

BUCKET_NAME = "kruger-bucket"
GATE_CODES_BUCKET_PATH = f"gs://{BUCKET_NAME}/landing/gate_codes/*.csv"
BQ_TABLE = "project-a2ce378b-71f9-4087-95b.bronze_dataset.kruger_gates"
TEMP_GCS_BUCKET = f"{BUCKET_NAME}/temp/"

kruger_gates_df = spark.read.csv(GATE_CODES_BUCKET_PATH, header=True)

#write to big query
kruger_gates_df.write.format("bigquery").option("table", BQ_TABLE).option("temporaryGcsBucket", TEMP_GCS_BUCKET).mode("overwrite").save()