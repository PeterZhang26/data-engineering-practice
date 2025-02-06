import os
import zipfile
import sys
import io
import shutil
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, to_date, month, year, desc, lit, row_number
from pyspark.sql.window import Window
from datetime import timedelta

# Set environment variable for UTF-8 encoding
os.environ["PYTHONIOENCODING"] = "utf-8"

# Manually set stdout encoding to UTF-8
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")


def extract_and_read_data(spark, data_path):
    """
    Extracts all zipped CSVs and reads them into a single DataFrame.
    """
    extracted_folder = "extracted_data"

    # Ensure extraction folder exists
    os.makedirs(extracted_folder, exist_ok=True)

    # Unzip all .zip files in the data folder
    for zip_filename in os.listdir(data_path):
        if zip_filename.endswith(".zip"):
            zip_path = os.path.join(data_path, zip_filename)
            with zipfile.ZipFile(zip_path, "r") as zip_ref:
                zip_ref.extractall(extracted_folder)

    # Read extracted CSV files into a single PySpark DataFrame
    df = spark.read.option("header", True) \
                   .option("inferSchema", True) \
                   .csv(f"{extracted_folder}/*.csv")

    print("✅ Data extracted and loaded successfully!")

    # Debug: Print schema safely
    try:
        df.printSchema()
    except UnicodeEncodeError:
        print("⚠️ Warning: Could not print schema due to encoding issues.")

    return df


### **📊 Data Analysis Functions**

def average_trip_duration_per_day(df):
    """
    Calculate the average trip duration per day.
    """
    return df.withColumn("date", to_date(col("start_time"))) \
             .groupBy("date") \
             .agg(avg("tripduration").alias("avg_trip_duration")) \
             .orderBy(desc("date"))


def trips_per_day(df):
    """
    Count the number of trips per day.
    """
    return df.withColumn("date", to_date(col("start_time"))) \
             .groupBy("date") \
             .agg(count("*").alias("total_trips")) \
             .orderBy("date")


def most_popular_start_station_per_month(df):
    """
    Find the most popular start station per month.
    """
    df_filtered = df.filter(col("start_time").isNotNull())  # Remove null values
    
    df_grouped = df_filtered.withColumn("month", month(col("start_time"))) \
                            .withColumn("year", year(col("start_time"))) \
                            .groupBy("year", "month", "from_station_name") \
                            .agg(count("*").alias("trip_count"))

    # Ensure year and month are not null
    df_final = df_grouped.filter(col("year").isNotNull() & col("month").isNotNull())

    # Select the most popular station per month
    window_spec = Window.partitionBy("year", "month").orderBy(col("trip_count").desc())
    df_top_station = df_final.withColumn("rank", row_number().over(window_spec)) \
                             .filter(col("rank") == 1) \
                             .drop("rank")  # Drop rank column after filtering

    return df_top_station.orderBy("year", "month")


def top_3_stations_last_2_weeks(df):
    """
    Find the top 3 stations with the most trips in the last 2 weeks.
    """
    latest_date = df.select(to_date(col("start_time")).alias("date")) \
                    .orderBy(desc("date")) \
                    .limit(1).collect()[0]["date"]  # Extracts as Python date

    two_weeks_ago = latest_date - timedelta(days=14)

    result = df.withColumn("date", to_date(col("start_time"))) \
               .filter(col("date") >= lit(two_weeks_ago)) \
               .groupBy("date", "from_station_name") \
               .agg(count("*").alias("trip_count")) \
               .orderBy("date", desc("trip_count")) \
               .limit(3)

    print("📊 Top 3 stations in last 2 weeks:")
    result.show(5)  # Debug output
    return result


def gender_trip_duration_comparison(df):
    """
    Compare average trip duration for Male vs Female riders.
    """
    # Ensure we only process valid gender values
    df_filtered = df.filter((col("gender") == "Male") | (col("gender") == "Female"))

    return df_filtered.groupBy("gender") \
             .agg(avg("tripduration").alias("avg_trip_duration")) \
             .orderBy(desc("avg_trip_duration"))


def top_10_ages_for_longest_and_shortest_trips(df):
    """
    Find the top 10 ages for longest and shortest trips.
    """
    # Ensure birthyear is valid & convert it to integer
    df = df.filter(col("birthyear").isNotNull()) \
           .withColumn("birthyear", col("birthyear").cast("int")) \
           .withColumn("age", (2024 - col("birthyear")).cast("int"))  # Ensure it's an integer

    # Filter out invalid ages (e.g., negative ages or older than 120)
    df = df.filter((col("age") > 0) & (col("age") <= 120))

    # Select top 10 ages for longest and shortest trips
    longest_trips = df.orderBy(desc("tripduration")).select("age").distinct().limit(10)
    shortest_trips = df.orderBy("tripduration").select("age").distinct().limit(10)

    return longest_trips, shortest_trips


### **📂 Save Results to CSV (With Proper Naming)**

def save_to_csv(df, report_name):
    """
    Save DataFrame as a single consolidated CSV in the 'reports/' folder.
    Removes temporary Spark files (_SUCCESS, .crc).
    """
    output_folder = f"reports/{report_name}"
    os.makedirs(output_folder, exist_ok=True)

    # Save CSV file (coalesced into one file)
    df.coalesce(1).write.mode("overwrite").option("header", "true").csv(output_folder)

    # Rename 'part-0000' to actual CSV name
    for file in os.listdir(output_folder):
        if file.startswith("part-"):
            os.rename(
                os.path.join(output_folder, file),
                os.path.join("reports", f"{report_name}.csv")
            )

    # Remove temporary Spark files (_SUCCESS, .crc)
    for file in os.listdir(output_folder):
        file_path = os.path.join(output_folder, file)
        if file.startswith("_") or file.endswith(".crc"):
            os.remove(file_path)

    # Remove the now-empty Spark output directory
    if os.path.exists(output_folder) and not os.listdir(output_folder):
        os.rmdir(output_folder)

    print(f"✅ Report saved: reports/{report_name}.csv")


### **🚀 Main Execution**
def main():
    """
    Main function to run all analyses using the provided Spark session.
    """
    spark = SparkSession.builder.appName("Exercise6").enableHiveSupport().getOrCreate()

    print("🚀 Loading data...")
    df = extract_and_read_data(spark, "data")
    df.printSchema()  # Debug: Show schema

    # Generate & save reports
    print("\n📊 Calculating reports...\n")

    reports = {
        "average_trip_duration_per_day": average_trip_duration_per_day(df),
        "trips_per_day": trips_per_day(df),
        "most_popular_start_station_per_month": most_popular_start_station_per_month(df),
        "top_3_stations_last_2_weeks": top_3_stations_last_2_weeks(df),
        "gender_trip_duration_comparison": gender_trip_duration_comparison(df),
    }

    # Save all reports
    for report_name, report_df in reports.items():
        save_to_csv(report_df, report_name)

    # Handle longest & shortest trips separately
    longest_trips, shortest_trips = top_10_ages_for_longest_and_shortest_trips(df)
    save_to_csv(longest_trips, "top_10_ages_longest_trips")
    save_to_csv(shortest_trips, "top_10_ages_shortest_trips")

    print("✅ All reports saved in 'reports/' folder.")
    spark.stop()


if __name__ == "__main__":
    main()
