import os
import zipfile
import sys
import io
import glob
import shutil
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, regexp_extract, split, rank, hash, desc, when
from pyspark.sql.window import Window

# Ensure UTF-8 encoding for console output
os.environ["PYTHONIOENCODING"] = "utf-8"
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")

# Constants
DATA_PATH = "data"
ZIP_FILE = "hard-drive-2022-01-01-failures.csv.zip"
EXTRACTED_FOLDER = os.path.join(DATA_PATH, "extracted_data")
PROCESSED_FOLDER = os.path.join(DATA_PATH, "processed_data")


def extract_csv_from_zip(data_path, zip_file):
    """
    Extracts the CSV file from a ZIP archive.
    """
    os.makedirs(EXTRACTED_FOLDER, exist_ok=True)  # Ensure folder exists
    zip_path = os.path.join(data_path, zip_file)

    try:
        with zipfile.ZipFile(zip_path, "r") as zip_ref:
            zip_ref.extractall(EXTRACTED_FOLDER)

        # Verify extraction
        extracted_files = os.listdir(EXTRACTED_FOLDER)
        csv_files = [f for f in extracted_files if f.endswith(".csv")]

        if not csv_files:
            raise FileNotFoundError("❌ No CSV file found after extraction!")

        print(f"✅ Extracted files: {csv_files}")
        return os.path.join(EXTRACTED_FOLDER, csv_files[0])  # Return CSV path

    except Exception as e:
        print(f"❌ Error extracting ZIP file: {e}")
        sys.exit(1)


def read_data(spark, csv_path):
    """
    Reads extracted CSV into a PySpark DataFrame.
    """
    df = spark.read.option("header", True).option("inferSchema", True).csv(csv_path)
    df = df.withColumn("source_file", lit(ZIP_FILE))  # Step 1: Add source_file column

    print(f"✅ Successfully loaded data from {csv_path}")
    return df


def transform_data(df):
    """
    Perform all required transformations on the DataFrame.
    """

    # Step 2: Extract date from source_file and convert to date format
    df = df.withColumn("file_date", regexp_extract(col("source_file"), r"(\d{4}-\d{2}-\d{2})", 1).cast("date"))

    # Step 3: Extract brand from model
    df = df.withColumn("brand", when(col("model").contains(" "), split(col("model"), " ")[0]).otherwise(lit("unknown")))

    # Step 4: Create storage ranking based on capacity_bytes per model
    window_spec = Window.orderBy(desc("capacity_bytes"))
    capacity_ranking_df = df.select("model", "capacity_bytes").distinct().withColumn("storage_ranking", rank().over(window_spec))

    df = df.join(capacity_ranking_df, on=["model", "capacity_bytes"], how="left")

    # Step 5: Create a unique primary key hash
    df = df.withColumn("primary_key", hash(col("serial_number"), col("date"), col("model")))

    return df


def clean_spark_output(output_path, output_file_name):
    """
    Cleans unnecessary Spark files and renames the CSV properly.
    """
    # Get list of output files
    files = glob.glob(os.path.join(output_path, "part-00000*"))
    
    if files:
        final_output_file = os.path.join(output_path, output_file_name)
        shutil.move(files[0], final_output_file)  # Rename the CSV file
        print(f"✅ Renamed output file: {final_output_file}")

    # Remove Spark temp files (_SUCCESS, .crc)
    temp_files = glob.glob(os.path.join(output_path, "_*")) + glob.glob(os.path.join(output_path, ".*"))
    for temp_file in temp_files:
        os.remove(temp_file)
        print(f"🗑️ Removed temp file: {temp_file}")


def save_transformed_data(df, output_path=PROCESSED_FOLDER, output_file_name="processed_hard_drive_data.csv"):
    """
    Save the transformed data as a properly named CSV file.
    """
    os.makedirs(output_path, exist_ok=True)  # Ensure output directory exists
    df.coalesce(1).write.mode("overwrite").csv(output_path, header=True)

    # Clean temp files and rename output CSV
    clean_spark_output(output_path, output_file_name)


def main():
    spark = SparkSession.builder.appName("Exercise7").enableHiveSupport().getOrCreate()

    print("🚀 Extracting CSV file...")
    csv_path = extract_csv_from_zip(DATA_PATH, ZIP_FILE)

    print("🔄 Loading Data...")
    df = read_data(spark, csv_path)

    print("🔄 Transforming Data...")
    transformed_df = transform_data(df)

    print("📊 Final Schema:")
    transformed_df.printSchema()

    # Save results
    save_transformed_data(transformed_df)

    print("✅ All processing complete!")
    spark.stop()


if __name__ == "__main__":
    main()
