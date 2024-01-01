from pyspark.sql import SparkSession, Row, Column
from pyspark.sql.types import StructType, StructField, StringType, FloatType, ArrayType
from pyspark.sql.functions import from_json, explode, col


def start_streaming(spark):
    try:
        stream_df = (
            spark.readStream.format("socket")
            .option("host", "localhost")
            .option("port", 9999)
            .load()
        )

        schema = StructType(
            [
                StructField("review_id", StringType()),
                StructField("user_id", StringType()),
                StructField("business_id", StringType()),
                StructField("stars", FloatType()),
                StructField("date", StringType()),
                StructField("text", StringType()),
            ]
        )

        # Parse the list of JSON strings into an array of structs
        stream_df = stream_df.withColumn(
            "json_data", from_json(col("value"), ArrayType(schema))
        )

        # Explode the array of structs into separate rows
        stream_df = stream_df.withColumn("data", explode(col("json_data")))

        # Select the individual fields from the struct
        stream_df = stream_df.select("data.*")

        query = (
            stream_df.writeStream.outputMode("append")
            .format("console")
            .options(truncate=False)
            .start()
        )
        query.awaitTermination()

    except Exception as e:
        print(e)


if __name__ == "__main__":
    spark_conn = SparkSession.builder.appName("SocketStreamConsumer").getOrCreate()

    start_streaming(spark_conn)
