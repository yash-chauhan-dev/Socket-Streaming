from time import sleep
import openai
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, FloatType, ArrayType
from pyspark.sql.functions import from_json, explode, col, udf, when
from config.config import config


def sentiment_analysis(comment) -> str:
    if comment:
        openai.api_key = config["openai"]["api_key"]
        completion = openai.ChatCompletion.create(
            model="gpt-3.5-turbo",
            messages=[
                {
                    "role": "system",
                    "content": """
                        You're a machine learning model with a task of classifying comments into POSITIVE, NEGATIVE, NEUTRAL.
                        You are to respond with one word from the option specified above, do not add anything else.
                        Here is the comment:
                        
                        {comment}
                    """.format(
                        comment=comment
                    ),
                }
            ],
        )
        return completion.choices[0].message["content"]
    return "Empty"


def start_streaming(spark):
    topic = "customers_review"
    try:
        stream_df = (
            spark.readStream.format("socket")
            .option("host", "0.0.0.0")
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

        sentiment_analysis_udf = udf(sentiment_analysis, StringType())

        stream_df = stream_df.withColumn(
            "feedback",
            when(
                col("text").isNotNull(), sentiment_analysis_udf(col("text"))
            ).otherwise(None),
        )

        kafka_df = stream_df.selectExpr(
            "CAST(review_id as STRING) AS key", "to_json(struct(*)) AS value"
        )

        query = (
            kafka_df.writeStream.format("KAFKA")
            .option("kafka.bootstrap.servers", config["KAFKA"]["bootstrap.servers"])
            .option("kafka.security.protocol", config["KAFKA"]["security.protocol"])
            .option("kafka.sasl.mechanism", config["KAFKA"]["sasl.mechanisms"])
            .option(
                "kafka.sasl.jaas.config",
                'org.apache.kafka.common.security.plain.PlainLoginModule required username="{username}" '
                'password="{password}";'.format(
                    username=config["KAFKA"]["sasl.username"],
                    password=config["KAFKA"]["sasl.password"],
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
