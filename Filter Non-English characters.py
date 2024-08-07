# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace, when, length
from pyspark.sql.types import StringType

# Initialize Spark session
spark = SparkSession.builder.appName("FilterNonEnglish").getOrCreate()

# Define the path to your CSV file
file_path = "dbfs:/FileStore/sample_data-2.csv"  # Update this path to your file location

# Read the CSV file into a DataFrame
df = spark.read.option("header", "true").csv(file_path)

# Define a regex pattern to match only English alphabet characters and basic punctuation
# This pattern matches characters from a to z (case-insensitive), spaces, and common punctuation
english_regex = "^[a-zA-Z0-9\s.,!?'\-()]*$"

# Function to check if the text contains only English characters using regex
def is_english_only(text):
    import re
    if text is None:
        return False
    return bool(re.match(english_regex, text))

# Register the function as a UDF
from pyspark.sql.functions import udf
from pyspark.sql.types import BooleanType

is_english_only_udf = udf(is_english_only, BooleanType())

# Apply the UDF to filter out rows with non-English characters
filtered_df = df.filter(
    is_english_only_udf(col("id")) &
    is_english_only_udf(col("name")) &
    is_english_only_udf(col("description"))
)

# Show the filtered DataFrame
filtered_df.show(truncate=False)

# Optionally, write the filtered DataFrame to a new CSV file
output_path = "/dbfs/path/to/filtered_data.csv"  # Update this path
filtered_df.write.mode("overwrite").option("header", "true").csv(output_path)


# COMMAND ----------

display(spark.read.csv("dbfs:/FileStore/sample_data-2.csv"))

# COMMAND ----------

# MAGIC %md
# MAGIC <h1> Below code is remove non-readable characters from the input dataset<h1>

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace

# Initialize Spark session
spark = SparkSession.builder.appName("RemoveNonReadableCharacters").getOrCreate()

# Define the path to your CSV file
file_path = "dbfs:/FileStore/sample_data-2.csv"  # Update this path

# Read the CSV file into a DataFrame
df = spark.read.option("header", "true").csv(file_path)

# Define a regex pattern to match only readable characters (printable ASCII characters)
# The regex pattern includes characters from space (0x20) to tilde (0x7E) in ASCII
readable_regex = "[^\\x20-\\x7E]"

# Apply regex replacement to clean non-readable characters
cleaned_df = df.select(
    *[regexp_replace(col(c), readable_regex, "").alias(c) for c in df.columns]
)

# Show the cleaned DataFrame
cleaned_df.show(truncate=False)

# Optionally, write the cleaned DataFrame to a new CSV file
output_path = "/dbfs/path/to/cleaned_data.csv"  # Update this path
cleaned_df.write.mode("overwrite").option("header", "true").csv(output_path)

