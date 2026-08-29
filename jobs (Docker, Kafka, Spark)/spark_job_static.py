from pyspark.sql import SparkSession
from Preprocessing.utils import load_csv
from Preprocessing.streams import preprocess_streams
from Preprocessing.clips import preprocess_clips
from Preprocessing.videos import preprocess_videos
from Preprocessing.top_games import preprocess_top_games
from Preprocessing.users import preprocess_users
from writes import write_to_gcs_batch, write_to_bq_batch

if __name__ == "__main__":

    spark = SparkSession.builder.appName('Client_Driver').getOrCreate()

    raw_streams = load_csv(spark, "gs://twitch-big-data/Raw_data/streams.csv")
    raw_clips = load_csv(spark, "gs://twitch-big-data/Raw_data/clips.csv")
    raw_videos = load_csv(spark, "gs://twitch-big-data/Raw_data/videos.csv")
    raw_top_games = load_csv(spark, "gs://twitch-big-data/Raw_data/top_games.csv")
    raw_users = load_csv(spark, "gs://twitch-big-data/Raw_data/users.csv")

    preprocessed_streams, stream_tags_df = preprocess_streams(raw_streams)
    preprocessed_clips = preprocess_clips(raw_clips)
    preprocessed_videos = preprocess_videos(raw_videos)
    preprocessed_top_games = preprocess_top_games(raw_top_games)
    preprocessed_users = preprocess_users(raw_users)

    write_to_gcs_batch(preprocessed_streams, "streams")
    write_to_gcs_batch(preprocessed_clips, "clips")
    write_to_gcs_batch(preprocessed_videos, "videos")
    write_to_gcs_batch(preprocessed_top_games, "top_games")
    write_to_gcs_batch(preprocessed_users, "users")
    write_to_gcs_batch(stream_tags_df, "stream_tags")

    write_to_bq_batch(preprocessed_streams, "streams")
    write_to_bq_batch(preprocessed_clips, "clips")
    write_to_bq_batch(preprocessed_videos, "videos")
    write_to_bq_batch(preprocessed_top_games, "top_games")
    write_to_bq_batch(preprocessed_users, "users")
    write_to_bq_batch(stream_tags_df, "stream_tags")
