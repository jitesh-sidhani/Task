# import time
# from pyspark.sql.functions import col, avg, from_json, to_json, struct , concat, lit , current_timestamp
# from pyspark.sql import SparkSession
# from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, DoubleType , LongType

# spark = SparkSession.builder.appName("Data").getOrCreate()

# # Define the schema for the Kafka data
# payload_after_schema = StructType([
#     StructField("year_month", StringType(), True),
#     StructField("Sales +", DoubleType(), True),
#     StructField("Expenses +", DoubleType(), True),
#     StructField("Operating profit", DoubleType(), True),
#     StructField("OPM %", DoubleType(), True),
#     StructField("Other Income +", DoubleType(), True),
#     StructField("Interest", DoubleType(), True),
#     StructField("Depreciation", DoubleType(), True),
#     StructField("Profit before tax", DoubleType(), True),
#     StructField("Tax %", DoubleType(), True),
#     StructField("Net Profit +", DoubleType(), True),
#     StructField("EPS in Rs", DoubleType(), True),
#     StructField("Dividend payout %", DoubleType(), True),
# ])

# # Define the schema for the new topic
# new_topic_schema = StructType([
#     StructField("before", StructType([]), True),
#     StructField("after", payload_after_schema, True),
#     StructField("source", StructType([
#         StructField("version", StringType(), True),
#         StructField("connector", StringType(), True),
#         StructField("name", StringType(), True),
#         StructField("ts_ms", LongType(), True),
#         StructField("snapshot", StringType(), True),
#         StructField("db", StringType(), True),
#         StructField("sequence", StringType(), True),
#         StructField("schema", StringType(), True),
#         StructField("table", StringType(), True),
#         StructField("txId", LongType(), True),
#         StructField("lsn", LongType(), True),
#         StructField("xmin", LongType(), True)
#     ]), True),
#     StructField("op", StringType(), True),
#     StructField("ts_ms", LongType(), True),
#     StructField("transaction", StructType([
#         StructField("id", StringType(), True),
#         StructField("total_order", LongType(), True),
#         StructField("data_collection_order", LongType(), True)
#     ]), True)
# ])

# payload_schema = StructType([StructField("after", payload_after_schema, True)])
# message_schema = StructType([StructField("payload", payload_schema, True)])

# df1 = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "kafka:9092").option("subscribe", "postgres.public.profit_loss_data_transposed").option("includeHeaders", "true").load()

# print("Schema of raw Kafka data:")
# df1.printSchema()

# raw_df = df1.selectExpr("CAST(value AS STRING) as json_value")

# print("Printing raw JSON values from Kafka:")
# query1 = raw_df.writeStream.format("console").option("truncate", "false").start()

# parsed_df = raw_df.select(from_json(col("json_value"), message_schema).alias("data")).select("data.payload.after.*")

# print("Printing parsed data after applying schema:")
# query2 = parsed_df.writeStream.format("console").option("truncate", "false").start()

# transformed_df = parsed_df.withColumn("bouns", concat(lit('abc'), col('year_month')))

# print("Printing transformed data to verify the transformation:")
# query3 = transformed_df.writeStream.format("console").option("truncate", "false").start()

# # Transform the data and convert it to JSON
# transformed_df = parsed_df.withColumn("bouns", concat(lit('abc'), col('year_month')))
# new_data_df = transformed_df.select(
#     lit(None).cast("string").alias("before"),
#     struct([
#         col("year_month"),
#         col("OPM %"),
#         col("Interest"),
#         col("Depreciation"),
#         col("Profit before tax"),
#         col("Tax %"),
#         col("EPS in Rs"),
#         col("bouns")
#     ]).alias("after"),
#     struct([
#         lit("2.5.4.Final").alias("version"),
#         lit("postgresql").alias("connector"),
#         lit("postgres").alias("name"),
#         current_timestamp().alias("ts_ms"),
#         lit("false").alias("snapshot"),
#         lit("exampledb").alias("db"),
#         lit("[\"24233736\",\"24234024\"]").alias("sequence"),
#         lit("public").alias("schema"),
#         lit("profit_loss_data_transposed").alias("table"),
#         lit(499).alias("txId"),
#         lit(24234024).alias("lsn"),
#         lit(None).alias("xmin")
#     ]).alias("source"),
#     lit("c").alias("op"),
#     current_timestamp().alias("ts_ms"),
#     lit(None).alias("transaction")
# )
# json_df = new_data_df.select(to_json(struct([col(c) for c in new_data_df.columns])).alias("value"))

# # Write the transformed JSON data to the new topic
# query4 = json_df.writeStream.format("kafka").option("kafka.bootstrap.servers", "kafka:9092").option("topic", "new_topic").option("checkpointLocation", "/tmp/checkpoints").start()

# # Convert the transformed data back to JSON
# # json_df = transformed_df.select(to_json(struct([col(c) for c in transformed_df.columns])).alias("value"))

# # query4 = json_df.writeStream.format("kafka").option("kafka.bootstrap.servers", "kafka:9092").option("topic", "new_topic").option("checkpointLocation", "/tmp/checkpoints").start()

# spark.streams.awaitAnyTermination()























































# import time
# from pyspark.sql.functions import col, avg  
# from pyspark.sql import SparkSession
# from pyspark.sql.functions import from_json, col
# from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType,DoubleType ,LongType
# from pyspark.sql.functions import concat, lit

# spark = SparkSession.builder \
#     .appName("Data") \
#     .getOrCreate()

# payload_after_schema = StructType([
#     StructField("index", LongType(), True),
#     StructField("year", StringType(), True),
#     StructField("sales", DoubleType(), True),
#     StructField("expenses", DoubleType(), True),
#     StructField("operating_profit", DoubleType(), True),
#     StructField("opm_percent", DoubleType(), True),
#     StructField("other_income", DoubleType(), True),
#     StructField("interest", DoubleType(), True),
#     StructField("depreciation", DoubleType(), True),
#     StructField("profit_before_tax", DoubleType(), True),
#     StructField("tax_percent", DoubleType(), True),
#     StructField("net_profit", DoubleType(), True),
#     StructField("eps_in_rs", DoubleType(), True),
#     StructField("dividend_payout_percent", DoubleType(), True),
#     StructField("stock", StringType(), True)
# ])

# payload_schema = StructType([
#     StructField("after", payload_after_schema, True)
# ])


# message_schema = StructType([
#     StructField("payload", payload_schema, True)
# ])


# df1 = spark.readStream \
#     .format("kafka") \
#     .option("kafka.bootstrap.servers", "kafka:9092") \
#     .option("subscribe", "postgres.public.profit_loss_data") \
#     .option("startingOffsets", "earliest") \
#     .option("includeHeaders", "true") \
#     .option("failOnDataLoss", "false") \
#     .load()

# print("Schema of raw Kafka data:")
# df1.printSchema()


# raw_df = df1.selectExpr("CAST(value AS STRING) as json_value")


# print("Printing raw JSON values from Kafka:")
# query1 = raw_df.writeStream \
#     .format("console") \
#     .option("truncate", "false") \
#     .start()


# parsed_df = raw_df.select(from_json(col("json_value"), message_schema).alias("data")) \
#     .select("data.payload.after.*")


# print("Printing parsed data after applying schema:")
# query2 = parsed_df.writeStream \
#     .format("console") \
#     .option("truncate", "false") \
#     .start()


# transformed_df = parsed_df.withColumn("bouns", concat(lit('abc'), col('year')))

# print("Printing transformed data to verify the transformation:")
# query3 = transformed_df.writeStream \
#     .format("console") \
#     .option("truncate", "false") \
#     .start()



# query4 = transformed_df.selectExpr( "to_json(struct(*)) AS value") \
#     .writeStream \
#     .format("kafka") \
#     .option("kafka.bootstrap.servers", "kafka:9092") \
#     .option("topic", "postgres.public.profit_loss_data") \
#     .option("checkpointLocation", "/tmp/checkpoints") \
#     .start()

# spark.streams.awaitAnyTermination()











































import time
from pyspark.sql.functions import col, avg  
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType,DoubleType ,LongType
from pyspark.sql.functions import concat, lit

spark = SparkSession.builder \
    .appName("Data") \
    .getOrCreate()


payload_after_schema = StructType([
    StructField("Section", StringType(), True),
    StructField("Mar-15", IntegerType(), True),
    StructField("Mar-16", StringType(), True),
    StructField("Mar-17", StringType(), True),
    StructField("Mar-18", StringType(), True),
    StructField("Mar-19", StringType(), True),
    StructField("Mar-20", StringType(), True),
    StructField("Mar-21", StringType(), True),
    StructField("Mar-22", IntegerType(), True),
    StructField("Mar-23", StringType(), True),
    StructField("Mar-24", IntegerType(), True),
    StructField("Stock_Code", StringType(), True)
])

payload_schema = StructType([
    StructField("after", payload_after_schema, True)
])


message_schema = StructType([
    StructField("payload", payload_schema, True)
])


df1 = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "postgtres.public.companies_data") \
    .option("startingOffsets", "earliest") \
    .option("includeHeaders", "true") \
    .option("failOnDataLoss", "false") \
    .load()

print("Schema of raw Kafka data:")
df1.printSchema()


raw_df = df1.selectExpr("CAST(value AS STRING) as json_value")


print("Printing raw JSON values from Kafka:")
query1 = raw_df.writeStream \
    .format("console") \
    .option("truncate", "false") \
    .start()


parsed_df = raw_df.select(from_json(col("json_value"), message_schema).alias("data")) \
    .select("data.payload.after.*")


print("Printing parsed data after applying schema:")
query2 = parsed_df.writeStream \
    .format("console") \
    .option("truncate", "false") \
    .start()


# transformed_df = parsed_df.withColumn("bouns", concat(lit('abc'), col('Section')))

print("Printing transformed data to verify the transformation:")
query3 = parsed_df.writeStream \
    .format("console") \
    .option("truncate", "false") \
    .start()


# Define PSQL JDBC properties
psql_jdbc_url = "jdbc:postgresql://postgres:5432/exampledb"
psql_jdbc_properties = {
    "user": "docker",
    "password": "docker",
    "driver": "org.postgresql.Driver"
}

def write_to_psql(batch_df, batch_id):
    try:
        print(f"Batch ID: {batch_id}")
        batch_df.show()
        batch_df.write.jdbc(
            url=psql_jdbc_url,
            table="new_target_table",
            mode="append",
            properties=psql_jdbc_properties
        )
        print(f"Batch {batch_id} written to PSQL successfully.")
    except Exception as e:
        print(f"Error writing batch {batch_id} to PSQL: {e}")

query5 = parsed_df.writeStream \
    .foreachBatch(write_to_psql) \
    .outputMode("append") \
    .start()


# query4 = transformed_df.selectExpr( "to_json(struct(*)) AS value") \
#     .writeStream \
#     .format("kafka") \
#     .option("kafka.bootstrap.servers", "kafka:9092") \
#     .option("topic", "postgres.public.profit_loss_data") \
#     .option("checkpointLocation", "/tmp/checkpoints") \
#     .start()

spark.streams.awaitAnyTermination()





























































# from pyspark.sql import SparkSession
# from pyspark.sql.functions import from_json, col, to_json, struct, expr


# from pyspark.sql.types import StructType, StructField, StringType, IntegerType
# # Create Spark session
# spark = SparkSession.builder \
#     .appName("ETL Profit and Loss") \
#     .getOrCreate()

# spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

# payload_after_schema = StructType([
#     StructField("Section", StringType(), True),
#     StructField("Mar-15", IntegerType(), True),
#     StructField("Mar-16", StringType(), True),
#     StructField("Mar-17", StringType(), True),
#     StructField("Mar-18", StringType(), True),
#     StructField("Mar-19", StringType(), True),
#     StructField("Mar-20", StringType(), True),
#     StructField("Mar-21", StringType(), True),
#     StructField("Mar-22", IntegerType(), True),
#     StructField("Mar-23", StringType(), True),
#     StructField("Mar-24", IntegerType(), True),
#     StructField("Stock_Code", StringType(), True)
# ])

# payload_schema = StructType([
#     StructField("after", payload_after_schema, True)
# ])


# message_schema = StructType([
#     StructField("payload", payload_schema, True)
# ])


# # Read data from Kafka
# df1 = spark.readStream \
#     .format("kafka") \
#     .option("kafka.bootstrap.servers", "kafka:9092") \
#     .option("subscribe", "postgtres.public.companies_data") \
#     .option("startingOffsets", "earliest") \
#     .option("includeHeaders", "true") \
#     .load()

# # print("Schema of raw Kafka data:")
# df1.printSchema()




# raw_df = df1.selectExpr("CAST(value AS STRING) as json_value")


# print("Printing raw JSON values from Kafka:")
# query1 = raw_df.writeStream \
#     .format("console") \
#     .option("truncate", "false") \
#     .start()


# parsed_df = raw_df.select(from_json(col("json_value"), message_schema).alias("data")) \
#     .select("data.payload.after.*")


# print("Printing parsed data after applying schema:")
# query2 = parsed_df.writeStream \
#     .format("console") \
#     .option("truncate", "false") \
#     .start()


# # transformed_df = parsed_df.withColumn("bouns", concat(lit('abc'), col('year')))

# # print("Printing transformed data to verify the transformation:")
# query3 = parsed_df.writeStream \
#     .format("console") \
#     .option("truncate", "false") \
#     .start()






# # Define PSQL JDBC properties
# # psql_jdbc_url = "jdbc:postgresql://postgres:5432/exampledb"
# # psql_jdbc_properties = {
# #     "user": "docker",
# #     "password": "docker",
# #     "driver": "org.postgresql.Driver"
# # }

# # def write_to_psql(batch_df, batch_id):
# #     try:
# #         print(f"Batch ID: {batch_id}")
# #         batch_df.show()
# #         batch_df.write.jdbc(
# #             url=psql_jdbc_url,
# #             table="new_target_table",
# #             mode="append",
# #             properties=psql_jdbc_properties
# #         )
# #         print(f"Batch {batch_id} written to PSQL successfully.")
# #     except Exception as e:
# #         print(f"Error writing batch {batch_id} to PSQL: {e}")

# # query5 = df1.writeStream \
# #     .foreachBatch(write_to_psql) \
# #     .outputMode("append") \
# #     .start()

# # spark.streams.awaitAnyTermination()