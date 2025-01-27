import requests
import pandas as pd
from bs4 import BeautifulSoup

url = "https://www.ncei.noaa.gov/data/local-climatological-data/access/2021/"


def main():
    response = requests.get(url)
    if response.status_code == 200:
        print("Successfully fetched the webpage!")
        print(response.text[:500])
    else:
        print(f"Failed to fetch the webpage. Status code: {response.status_code}")

    # Parse the HTML with BeautifulSoup
    soup = BeautifulSoup(response.content, "html.parser")

    # Find all rows in the table (assuming files are listed in rows <tr>)
    rows = soup.find_all("tr")
    print(f"Found {len(rows)} rows in the table.")

    # Debug: Print the first few rows to inspect
    for row in rows[:5]: # Print the first 5 rows
        print(row)
        print("-" * 40)

    # Locate the file with the matching timestamp
    target_filename = None
    for row in rows:
        link = row.find("a")
        if link:
            filename = link.text # Extract the filename
            cols = row.find_all("td") # Find all columns in the row
            if len(cols) > 1: # Ensure there are enough columns
                # Debug: Print raw timestamp text
                raw_timestamp = cols[1].text
                print(f"Raw timestamp text: '{raw_timestamp}'")

                # Clean the timestamp: normalize spaces and ensure a single space between date and time
                cleaned_timestamp = " ".join(raw_timestamp.split())
                print(f"Cleaned timestamp: '{cleaned_timestamp}'")

                # Match the timestamp with the desired one
                if cleaned_timestamp == "2024-01-19 10:27":
                    print(f"Match found! Filename: {filename}")
                    target_filename = filename
                    break

    if not target_filename:
        print("No file found with specified timestamp.")
        return
    
    # Build the file URL and download the file
    file_url = url + target_filename
    print(f"Downloading the file from {file_url}...")
    file_response = requests.get(file_url)
    if file_response.status_code == 200:
        local_filename = target_filename
        with open(local_filename, "wb") as f:
            f.write(file_response.content)
        print(f"File download and saved as {local_filename}")
    else:
        print(f"Failed to download the file. Status code: {file_response.status_code}")
        return
    
    # Load the file with Pandas
    print(f"Loading {local_filename} into Pandas...")
    try:
        df = pd.read_csv(local_filename)

        # Find the record(s) with the highest `HourlyDryBulbTemperature`
        if "HourlyDryBulbTemperature" in df.columns:
            max_temp = df["HourlyDryBulbTemperature"].max()
            max_records = df[df["HourlyDryBulbTemperature"] == max_temp]
            print("Record(s) with the highest HourlyDryBulbTemperature:")
            print(max_records)
        else:
            print("Column `HourlyDryBulbTemperature` not found in the data.")
    
    except Exception as e:
        print(f"Error loading file into Pandas: {e}")

if __name__ == "__main__":
    main()

