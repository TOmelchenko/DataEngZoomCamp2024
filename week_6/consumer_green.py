import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import types
from pyspark.sql import functions as F
from pyspark.sql.functions import current_timestamp

schema = types.StructType([
    types.StructField('lpep_pickup_datetime', types.StringType(), True),
    types.StructField('lpep_dropoff_datetime', types.StringType(), True),
    types.StructField('PULocationID', types.IntegerType(), True),
    types.StructField('DOLocationID', types.IntegerType(), True),    
    types.StructField('passenger_count', types.StringType(), True),
    types.StructField('trip_distance', types.DoubleType(), True),
    types.StructField('total_amount', types.DoubleType(), True),
    types.StructField('curr_timestamp',types.StringType(), True)
])

pyspark_version = pyspark.__version__
kafka_jar_package = f"org.apache.spark:spark-sql-kafka-0-10_2.12:{pyspark_version}"

spark = SparkSession.builder.master("local[*]").appName('GreenTripsConsumer').\
    config("spark.jars.packages", kafka_jar_package).getOrCreate()

green_stream = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "green-trips") \
    .option("startingOffsets", "earliest")\
    .option("checkpointLocation", "checkpoint") \
    .load()

def debug_and_process(batch_df, batch_id):
    batch_df.select(F.col("value").cast('STRING')).show(truncate=False)
    parsed_batch = batch_df.select(F.from_json(F.col("value").cast('STRING'), schema).alias("data"))\
    .select("data.*")
    
    parsed_batch = parsed_batch.withColumn("curr_timestamp", current_timestamp()) 

    print(f"Batch ID: {batch_id}")
    parsed_batch.show(truncate=False)

query = green_stream \
    .writeStream \
    .foreachBatch(debug_and_process) \
    .start()




query.awaitTermination()