from pyspark.sql.functions import udf, col, trim
from pyspark.sql.types import StringType
import emoji
import unicodedata
import langcodes


def trim_string_columns(df):
    # Get all columns that have the string data type
    string_cols = [c for c, t in df.dtypes if t == "string"]

    # Loop through every string column
    for c in string_cols:
        # Remove leading and trailing spaces from the column values
        df = df.withColumn(c, trim(col(c)))

    # Return the DataFrame after trimming all string columns
    return df


def load_csv(spark, path):
    # Load a CSV file into a Spark DataFrame
    # header = true means the first row contains column names
    # quote and escape handle double quotes inside CSV values
    # multiLine = true allows one CSV field to contain multiple lines
    return (
        spark.read
        .option("header", "true")
        .option("quote", '"')
        .option("escape", '"')
        .option("multiLine", "true")
        .csv(path)
    )


def clean_title(text):
    # If the text is empty or None, return None
    if not text:
        return None

    # Remove emojis from the text
    cleaned = emoji.replace_emoji(text, replace='')

    # Remove punctuation characters from the text
    cleaned = "".join(
        ch for ch in cleaned
        if not unicodedata.category(ch).startswith('P')
    )

    # Remove leading and trailing spaces, then return the cleaned text
    return cleaned.strip()


# Convert the normal Python clean_title function into a Spark UDF
# This allows clean_title to be used on Spark DataFrame columns
clean_title_udf = udf(clean_title, StringType())


def get_lang_name(code):
    # If the language code is empty or None, return None
    if not code:
        return None

    try:
        # Convert a language code into a full language name
        # Example: "en" becomes "English"
        return langcodes.Language.get(code).display_name()

    except:
        # If conversion fails, return the original code
        return code


# Convert the normal Python get_lang_name function into a Spark UDF
# This allows get_lang_name to be used on Spark DataFrame columns
lang_name_udf = udf(get_lang_name, StringType())