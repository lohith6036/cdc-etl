import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, expr
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType, BooleanType

def main():
    print("Initializing Spark Structured Streaming Session with Iceberg and Kafka dependencies...")
    spark = SparkSession.builder \
        .appName("MySQL_to_Iceberg_Streaming_CDC") \
        .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.local.type", "hadoop") \
        .config("spark.sql.catalog.local.warehouse", f"{os.getcwd()}/spark-warehouse") \
        .getOrCreate()
        
    spark.sparkContext.setLogLevel("ERROR")

    # Define schema for the 'after' and 'before' payloads from Debezium
    customer_schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("updated_at", StringType(), True), # Often comes as a string representation of timestamp in MS or full ISO format
        StructField("is_deleted", BooleanType(), True)
    ])

    # Debezium payload schema
    payload_schema = StructType([
        StructField("before", customer_schema, True),
        StructField("after", customer_schema, True),
        StructField("op", StringType(), True) # c=create, u=update, d=delete
    ])

    def merge_batch(batch_df, batch_id):
        print(f"Processing micro-batch {batch_id}...")
        
        if batch_df.count() == 0:
            print("Empty batch, skipping.")
            return

        batch_df.createOrReplaceTempView("raw_updates")

        merge_sql = """
        MERGE INTO local.cdc_db.customers target
        USING (
            SELECT id, name, email, updated_at, is_deleted
            FROM (
                SELECT 
                    IF(op = 'd', before.id, after.id) as id,
                    IF(op = 'd', before.name, after.name) as name,
                    IF(op = 'd', before.email, after.email) as email,
                    CAST(IF(op = 'd', before.updated_at, after.updated_at) AS TIMESTAMP) as updated_at,
                    IF(op = 'd', true, IF(after.is_deleted IS NULL, false, after.is_deleted)) as is_deleted,
                    ROW_NUMBER() OVER (
                        PARTITION BY IF(op = 'd', before.id, after.id) 
                        ORDER BY CAST(IF(op = 'd', before.updated_at, after.updated_at) AS TIMESTAMP) DESC
                    ) as rn
                FROM raw_updates
            )
            WHERE rn = 1
        ) source
        ON target.id = source.id
        WHEN MATCHED AND source.is_deleted = true THEN DELETE
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED AND source.is_deleted = false THEN INSERT *
        """
        
        batch_df.sparkSession.sql(merge_sql)
        print("Micro-batch merge complete. Current target table state:")
        batch_df.sparkSession.sql("SELECT * FROM local.cdc_db.customers ORDER BY id").show(truncate=False)


    # Read from Kafka
    print("Starting streaming read from Kafka topic 'cdc_server.cdc_demo.customers'...")
    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "cdc_server.cdc_demo.customers") \
        .option("startingOffsets", "earliest") \
        .load()

    # The value comes as bytes containing a JSON string.
    value_df = kafka_df.selectExpr("CAST(value AS STRING) as json_payload")

    # Extract the 'payload' field from the Debezium JSON
    cdc_df = value_df \
        .withColumn("payload", from_json(col("json_payload"), StructType([StructField("payload", payload_schema, True)]))) \
        .select(col("payload.payload.*")) \
        .filter(col("op").isNotNull()) 

    # Write Stream using foreachBatch
    query = cdc_df.writeStream \
        .foreachBatch(merge_batch) \
        .outputMode("update") \
        .option("checkpointLocation", f"{os.getcwd()}/spark-checkpoint") \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    main()
