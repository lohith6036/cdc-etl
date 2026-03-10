import os
from pyspark.sql import SparkSession

def main():
    print("Initializing Spark Session with Iceberg and MySQL dependencies...")
    spark = SparkSession.builder \
        .appName("MySQL_to_Iceberg_CDC") \
        .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,com.mysql:mysql-connector-j:8.3.0") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.local.type", "hadoop") \
        .config("spark.sql.catalog.local.warehouse", f"{os.getcwd()}/spark-warehouse") \
        .getOrCreate()
        
    spark.sparkContext.setLogLevel("ERROR")

    # 1. Ensure the namespace (database) exists in Iceberg local catalog
    spark.sql("CREATE NAMESPACE IF NOT EXISTS local.cdc_db")
    
    # 2. Extract data from MySQL
    print("Reading data from MySQL 'cdc_demo.customers' table...")
    df_mysql = spark.read \
        .format("jdbc") \
        .option("url", "jdbc:mysql://localhost:3306/cdc_demo") \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .option("dbtable", "customers") \
        .option("user", "root") \
        .option("password", "rootpassword") \
        .load()
        
    df_mysql.createOrReplaceTempView("source_updates")
    
    # 3. Create target Iceberg table if it doesn't exist
    print("Ensuring Iceberg target table exists...")
    spark.sql("""
    CREATE TABLE IF NOT EXISTS local.cdc_db.customers (
        id INT,
        name STRING,
        email STRING,
        updated_at TIMESTAMP,
        is_deleted BOOLEAN
    )
    USING iceberg
    TBLPROPERTIES (
        'format-version'='2',
        'write.update.mode'='merge-on-read',
        'write.merge.mode'='merge-on-read'
    )
    """)
    
    # 4. Perform CDC Merge
    # We use the 'updated_at' column to potentially only update if it's newer, 
    # but for simplicity we'll merge on 'id'.
    print("Performing MERGE into Iceberg...")
    merge_sql = """
    MERGE INTO local.cdc_db.customers target
    USING source_updates source
    ON target.id = source.id
    WHEN MATCHED AND source.is_deleted = true THEN DELETE
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED AND source.is_deleted = false THEN INSERT *
    """
    spark.sql(merge_sql)
    
    print("CDC Pipeline Batch Execution Completed.")
    print("Current records in Iceberg:")
    spark.sql("SELECT * FROM local.cdc_db.customers ORDER BY id").show(truncate=False)

if __name__ == "__main__":
    main()
