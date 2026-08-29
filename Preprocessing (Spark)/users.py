from pyspark.sql.functions import col, when
from Preprocessing.utils import clean_title_udf, trim_string_columns
from pyspark.sql import DataFrame


def preprocess_users(users_df: DataFrame) -> DataFrame:
    # Remove duplicate rows
    users_df = users_df.dropDuplicates()

    # Remove leading and trailing spaces from all string columns
    users_df = trim_string_columns(users_df)

    # Create a new column that shows whether the user has an offline image
    # If offline_image_url is null, set has_offline_image to 0
    # Otherwise, set has_offline_image to 1
    users_df = users_df.withColumn(
        "has_offline_image",
        when(col("offline_image_url").isNull(), 0).otherwise(1)
    )

    # Remove columns that are not needed for analysis or storage
    users_df = users_df.drop(
        "type",
        "offline_image_url",
        "profile_image_url"
    )

    # Clean the description text using a custom UDF
    users_df = users_df.withColumn(
        "description",
        clean_title_udf(col("description"))
    )

    # Remove rows where the required user id is null
    users_df = users_df.dropna(
        subset=["id"]
    )

    # Return the cleaned users DataFrame
    return users_df
