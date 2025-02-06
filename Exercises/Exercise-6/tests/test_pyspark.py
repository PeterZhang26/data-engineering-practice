import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import sys
import os

# Add the parent directory (Exercise-6) to the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from main import (
    average_trip_duration_per_day,
    trips_per_day,
    most_popular_start_station_per_month,
    top_3_stations_last_2_weeks,
    gender_trip_duration_comparison,
    top_10_ages_for_longest_and_shortest_trips
)

@pytest.fixture(scope="session")
def spark():
    """
    Creates a new PySpark session for testing.
    """
    return SparkSession.builder.master("local").appName("PySparkTest").getOrCreate()


@pytest.fixture
def sample_data(spark):
    """
    Creates a mock DataFrame for testing.
    """
    data = [
        (1, "2019-10-01 00:01:39", "2019-10-01 00:17:20", 2215, 940.0, 20, "Station A", 309, "Station B", "Subscriber", "Male", 1987),
        (2, "2019-10-02 00:02:16", "2019-10-02 00:06:34", 6328, 258.0, 19, "Station B", 241, "Station C", "Subscriber", "Female", 1998),
        (3, "2019-10-03 00:05:00", "2019-10-03 00:25:00", 4000, 1200.0, 21, "Station A", 320, "Station D", "Subscriber", "Male", 1992),
    ]
    
    columns = ["trip_id", "start_time", "end_time", "bikeid", "tripduration", "from_station_id",
               "from_station_name", "to_station_id", "to_station_name", "usertype", "gender", "birthyear"]
    
    return spark.createDataFrame(data, columns)

@pytest.fixture(scope="function", autouse=True)
def reset_pytest_capture():
    pytest.stdout = None
    pytest.stderr = None


### **🧪 UNIT TESTS**

def test_average_trip_duration_per_day(spark, sample_data):
    result_df = average_trip_duration_per_day(sample_data)
    assert result_df.count() == 3  # Should have an entry for each day


def test_trips_per_day(spark, sample_data):
    result_df = trips_per_day(sample_data)
    assert result_df.count() == 3  # Should have an entry for each day
    assert result_df.columns == ["date", "total_trips"]


def test_most_popular_start_station_per_month(spark, sample_data):
    result_df = most_popular_start_station_per_month(sample_data)
    assert result_df.count() == 2  # Expecting two stations (Station A and Station B)
    assert "from_station_name" in result_df.columns


def test_top_3_stations_last_2_weeks(spark, sample_data):
    result_df = top_3_stations_last_2_weeks(sample_data)
    assert result_df.count() <= 3  # Should have at most 3 entries
    assert "from_station_name" in result_df.columns


def test_gender_trip_duration_comparison(spark, sample_data):
    result_df = gender_trip_duration_comparison(sample_data)
    assert result_df.count() == 2  # Male & Female
    assert "avg_trip_duration" in result_df.columns


def test_top_10_ages_for_longest_and_shortest_trips(spark, sample_data):
    longest_trips, shortest_trips = top_10_ages_for_longest_and_shortest_trips(sample_data)
    
    assert longest_trips.count() <= 10  # Should return at most 10 ages
    assert shortest_trips.count() <= 10
    assert "age" in longest_trips.columns
    assert "age" in shortest_trips.columns
