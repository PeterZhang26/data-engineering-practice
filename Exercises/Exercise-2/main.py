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

    # Parse the HTML with BeaufiulSoup
    soup = BeautifulSoup(response.content, "html.parser")

    # Find all rows in the table (assuming files are listed in rows <tr>)
    rows = soup.find_all("a")
    print(f"Found {len(rows)} rows in the table.")

    # Debug: Print the first few rows to inspect
    for row in rows[:5]: # Print the first 5 rows
        print(row)
        print("-" * 40)

if __name__ == "__main__":
    main()
