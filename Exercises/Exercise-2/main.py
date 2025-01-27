import requests
import pandas
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

    # Now loop through the rows to find the filename and timestamp
    for row in rows:
        # Find the <a> tag within the row (if it exists)
        link = row.find("a")
        if link:
            filename = link.text # Extract filename text
            print(f"Filename: {filename}")

            # Find the Last Modified timestamp
            cols = row.find_all("td") # Find all columns in the row
            if len(cols) > 1: # Ensure there are enough columns
                timestamp = cols[1].text.strip() # Assuming the timestamp is in the 2nd column
                print(f"Timestamp: {timestamp}")

                # Match the timestamp with the desired one
                if timestamp == "2024-01-19 10:27":
                    print(f"Match found! Filename: {filename}")
                    break

if __name__ == "__main__":
    main()
