from pyspark.sql.functions import col, split, explode, regexp_replace, to_timestamp
from pyspark.sql.types import BooleanType, LongType
from Preprocessing.utils import clean_title_udf, lang_name_udf, trim_string_columns
from pyspark.sql import DataFrame


def preprocess_streams(streams_df: DataFrame) -> DataFrame:
    # Remove the thumbnail_url column because it is not needed for analysis or storage
    streams_df = streams_df.drop("thumbnail_url")

    # Remove leading and trailing spaces from all string columns
    streams_df = trim_string_columns(streams_df)

    # Convert viewer_count to LongType
    # Convert is_mature to BooleanType
    # Convert started_at from string format to timestamp format
    streams_df = streams_df.withColumn(
        "viewer_count",
        col("viewer_count").cast(LongType())
    ).withColumn(
        "is_mature",
        col("is_mature").cast(BooleanType())
    ).withColumn(
        "started_at",
        to_timestamp(col("started_at"))
    )

    # Clean the tags column (RE):
    # 1. Remove square brackets []
    # 2. Remove single quotes '
    # 3. Split the string into an array using comma + space as the separator
    streams_df = streams_df.withColumn(
        "tags",
        split(
            regexp_replace(
                regexp_replace(col("tags"), r"[\[\]]", ""),
                r"'",
                ""
            ),
            ", "
        )
    )

    # Create a separate DataFrame for stream tags
    # Each tag becomes its own row
    stream_tags_df = streams_df.select(
        col("id").alias("stream_id"),
        explode(col("tags")).alias("tag")
    )

    # Remove tags and tag_ids from the main streams DataFrame
    # Tags are already stored separately in stream_tags_df
    streams_df = streams_df.drop("tags", "tag_ids")

    # Clean the title text using a custom UDF
    streams_df = streams_df.withColumn(
        "title",
        clean_title_udf(col("title"))
    )

    # Convert language code/name into a standardized language name using a custom UDF
    streams_df = streams_df.withColumn(
        "language",
        lang_name_udf(col("language"))
    )

    # Remove duplicate rows
    streams_df = streams_df.dropDuplicates()

    # Remove rows where important required fields are null
    streams_df = streams_df.dropna(
        subset=["id", "user_id", "started_at", "viewer_count"]
    )

    # Return the cleaned streams DataFrame and the separate stream tags DataFrame
    return streams_df, stream_tags_df