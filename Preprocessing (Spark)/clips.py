from pyspark.sql import DataFrame
from pyspark.sql.functions import col, to_timestamp
from Preprocessing.utils import clean_title_udf, lang_name_udf, trim_string_columns


def preprocess_clips(clips_df: DataFrame) -> DataFrame:
    # Remove columns that are not needed for analysis or storage
    clips_df = clips_df.drop('thumbnail_url', 'embed_url', 'url')

    # Remove extra columns that are not needed after preprocessing
    clips_df = clips_df.drop('vod_offset', 'video_id')


    # Remove leading and trailing spaces from all string columns
    clips_df = trim_string_columns(clips_df)

    # Convert view_count to long/integer type
    # Convert created_at from string format to timestamp format
    # Clean the title text using a custom UDF
    clips_df = clips_df.withColumn(
        "view_count",
        col("view_count").cast("long")
    ).withColumn(
        "created_at",
        to_timestamp(col("created_at"))
    ).withColumn(
        "title",
        clean_title_udf(col("title"))
    )

    # Convert language code/name into a standardized language name using a custom UDF
    clips_df = clips_df.withColumn(
        "language",
        lang_name_udf(col("language"))
    )

    # Remove duplicate rows
    clips_df = clips_df.dropDuplicates()

    # Remove rows where important required fields are null
    clips_df = clips_df.dropna(
        subset=["id", "broadcaster_id", "created_at", "view_count"]
    )

    # Return the cleaned DataFrame
    return clips_df