from pyspark.sql.functions import col, to_timestamp, regexp_extract
from Preprocessing.utils import clean_title_udf, lang_name_udf, trim_string_columns
from pyspark.sql import DataFrame


def preprocess_videos(videos_df: DataFrame) -> DataFrame:
    # Remove leading and trailing spaces from all string columns
    videos_df = trim_string_columns(videos_df)

    # Remove columns that are not needed for analysis or storage
    videos_df = videos_df.drop(
        'description',
        'muted_segments',
        'url',
        'thumbnail_url'
    )

    # Convert view_count to long/integer type
    # Convert created_at from string format to timestamp format
    # Convert published_at from string format to timestamp format
    # Extract the numeric part from duration and convert it to integer
    videos_df = (
        videos_df
        .withColumn(
            "view_count",
            col("view_count").cast("long")
        )
        .withColumn(
            "created_at",
            to_timestamp(col("created_at"))
        )
        .withColumn(
            "published_at",
            to_timestamp(col("published_at"))
        )
        .withColumn(
            "duration",
            regexp_extract(col("duration"), r'(\d+)', 1).cast("int")
        )
    )

    # Clean the title text using a custom UDF
    videos_df = videos_df.withColumn(
        "title",
        clean_title_udf(col("title"))
    )

    # Convert language code/name into a standardized language name using a custom UDF
    videos_df = videos_df.withColumn(
        "language",
        lang_name_udf(col("language"))
    )

    # Remove duplicate rows
    videos_df = videos_df.dropDuplicates()

    # Remove rows where important required fields are null
    videos_df = videos_df.dropna(
        subset=["id", "user_id", "created_at", "view_count"]
    )

    # Return the cleaned videos DataFrame
    return videos_df