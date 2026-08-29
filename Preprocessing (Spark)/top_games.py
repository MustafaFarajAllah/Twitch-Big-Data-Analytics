from Preprocessing.utils import trim_string_columns
from pyspark.sql import DataFrame


def preprocess_top_games(top_games_df: DataFrame) -> DataFrame:
    # Remove the box_art_url column because it is not needed for analysis or storage
    top_games_df = top_games_df.drop('box_art_url')

    # Remove leading and trailing spaces from all string columns
    top_games_df = trim_string_columns(top_games_df)

    # Remove duplicate rows
    top_games_df = top_games_df.dropDuplicates()

    # Remove rows where important required fields are null
    top_games_df = top_games_df.dropna(
        subset=["id", "name"]
    )

    # Return the cleaned top games DataFrame
    return top_games_df
