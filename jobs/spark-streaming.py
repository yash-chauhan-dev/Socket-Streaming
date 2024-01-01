from time import sleep
from pyspark.sql import SparkSession, Row, Column
from pyspark.sql.types import StructType, StructField, StringType, FloatType, ArrayType
from pyspark.sql.functions import from_json, explode, col
from config.config import config


def start_streaming(spark):
    topic = "customers_review"
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

        kafka_df = stream_df.selectExpr(
            "CAST(review_id as STRING) AS key", "to_json(struct(*)) AS value"
        )

        query = (
            kafka_df.writeStream.format("kafka")
            .option("kafka.bootstrap.servers", config["kafka"]["bootstrap.servers"])
            .option("kafka.security.protocol", config["kafka"]["security.protocol"])
            .option("kafka.sasl.mechanism", config["kafka"]["sasl.mechanisms"])
            .option(
                "kafka.sasl.jaas.config",
                'org.apache.kafka.common.security.plain.PlainLoginModule required username="{username}" '
                'password="{password}";'.format(
                    username=config["kafka"]["sasl.username"],
                    password=config["kafka"]["sasl.password"],
                ),
            )
            .option("checkpointLocation", "/tmp/checkpoint")
            .option("topic", topic)
            .start()
            .awaitTermination()
        )

    except Exception as e:
        print(f"Exception encountered: {e}. Retrying in 10 seconds")
        sleep(10)


if __name__ == "__main__":
    spark_conn = SparkSession.builder.appName("SocketStreamConsumer").getOrCreate()

    start_streaming(spark_conn)
