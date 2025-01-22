import requests
import os
import zipfile

download_uris = [
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2018_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q2.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q3.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2020_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2220_Q1.zip",
]


def main():
    # Create download folder directory if it doesn't exist
    target_path = os.path.join(os.getcwd(), "downloads")
    print(f"Target path: {target_path}")

    if os.path.exists(target_path):
        os.mkdir(target_path)

    # Download the files specified in URIs list
    for uri in download_uris:
        filename = os.path.basename(uri)
        filepath = os.path.join(target_path, filename)
    try:
        response = requests.get(uri, timeout=10)
        if response.status_code == 200:
            with open(filepath, "wb") as f:
                f.write(response.content)
            print(f"{filename} downloaded successfully.")
        else:
            print(f"Failed to download {filename}: {response.status_code}")
    except requests.RequestException as e:
        print(f"Error downloading {uri}: {e}")

    print("Download complete.")

    # Unzip files from the URIs

    for dirpath, dirnames, filenames in os.walk(target_path):
        for f in filenames:
            if f.endswith(".zip"):
                zip_path = os.path.join(dirpath, f)
                try:
                    with zipfile.ZipFile(zip_path, "r") as zip_ref:
                        zip_ref.extractall(dirpath)
                    os.remove(zip_path)
                    print(f"{f} extracted and removed.")
                except zipfile.BadZipFile:
                    print(f"{f} could not be extracted. Possibly invalid file.")
                except Exception as e:
                    print(f"Error extracting {f}: {e}")


if __name__ == "__main__":
    main()
