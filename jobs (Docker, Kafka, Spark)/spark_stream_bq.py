from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from Preprocessing.streams import preprocess_streams
from Preprocessing.users import preprocess_users
from Preprocessing.videos import preprocess_videos
from Preprocessing.clips import preprocess_clips
from Preprocessing.top_games import preprocess_top_games
from writes import write_to_bq_stream
from schemas import stream_schema, user_schema, video_schema, clip_schema, top_game_schema
from google.cloud import secretmanager


def read_kafka_topic(spark, topic, schema):
    raw_df = spark.readStream.format('kafka').option("kafka.bootstrap.servers", bootstrap_servers).option("subscribe", topic).option("startingOffsets", "earliest").option("kafka.security.protocol", "SASL_SSL").option("kafka.sasl.jaas.config", jaas_config).option("kafka.sasl.mechanism", "PLAIN").load()
    return raw_df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")


client = secretmanager.SecretManagerServiceClient()
get_secret = lambda s: client.access_secret_version(request={"name": f"projects/twitch-data-493815/secrets/{s}/versions/latest"}).payload.data.decode("UTF-8")

bootstrap_servers = get_secret("CONFLUENT_BOOTSTRAP_SERVERS")
api_key = get_secret("CONFLUENT_API_KEY")
api_secret = get_secret("CONFLUENT_API_SECRET")
jaas_config = f'org.apache.kafka.common.security.plain.PlainLoginModule required username="{api_key}" password="{api_secret}";'


if __name__ == "__main__":

    spark = SparkSession.builder.appName("SparkConsumer-BQ").config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,io.delta:delta-spark_2.12:3.2.0").config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension").config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog").getOrCreate()

    stream_df = read_kafka_topic(spark, "stream", stream_schema)
    user_df = read_kafka_topic(spark, "user", user_schema)
    video_df = read_kafka_topic(spark, "video", video_schema)
    clip_df = read_kafka_topic(spark, "clip", clip_schema)
    top_game_df = read_kafka_topic(spark, "top_game", top_game_schema)

    stream_df, stream_tags_df = preprocess_streams(stream_df)
    user_df = preprocess_users(user_df)
    video_df = preprocess_videos(video_df)
    clip_df = preprocess_clips(clip_df)
    top_game_df = preprocess_top_games(top_game_df)

    write_to_bq_stream(stream_df, "streams")
    write_to_bq_stream(stream_tags_df, "stream_tags")
    write_to_bq_stream(user_df, "users")
    write_to_bq_stream(video_df, "videos")
    write_to_bq_stream(clip_df, "clips")
    write_to_bq_stream(top_game_df, "top_games")

    spark.streams.awaitAnyTermination()


