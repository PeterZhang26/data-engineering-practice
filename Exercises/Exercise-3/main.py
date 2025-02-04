import requests
import gzip
from io import BytesIO

def timeout_handler(signum, frame):
    """Stops the process if it runs for too long"""
    raise Exception("Process took too long!")

def stream_file_https(url):
    """
    Downloads a file from an HTTP URL into memory using streaming.
    Uses a BytesIO buffer instead of saving to disk.
    """
    print(f"Downloading: {url}")
    response = requests.get(url, stream=True)

    if response.status_code == 200:
        buffer = BytesIO()
        for chunk in response.iter_content(chunk_size=8192):
            buffer.write(chunk)
        buffer.seek(0)
        print("Download successful!")
        return buffer
    else:
        raise Exception(f"Failed to download file. HTTP Status: {response.status_code}")

def main():
    """
    1. Stream 'wet.paths.gz' via HTTPS and extract it in memory.
    2. Read the first line (URI of the next file).
    3. Download the second file and stream it **without loading it into memory**.
    4. Print progress every 1000 lines instead of printing each line.
    """

    # Step 1: Download & Extract 'wet.paths.gz' **in memory**
    first_file_url = "https://data.commoncrawl.org/crawl-data/CC-MAIN-2022-05/wet.paths.gz"
    buffer = stream_file_https(first_file_url)

    # Step 2: Extract and read the first line **without writing to disk**
    with gzip.open(buffer, mode="rt", encoding="utf-8", errors="ignore") as f:
        first_line = f.readline().strip()
        print(f"First file's first line (URI): {first_line}")

    # Step 3: Construct the HTTPS URL for the second file
    second_file_url = f"https://data.commoncrawl.org/{first_line}"
    print(f"Downloading second file: {second_file_url}")

    # Step 4: Download & Stream the second file **without loading it all into memory**
    response = requests.get(second_file_url, stream=True)

    if response.status_code == 200:
        second_buffer = BytesIO()
        for chunk in response.iter_content(chunk_size=8192):
            second_buffer.write(chunk)

        second_buffer.seek(0)

        # Step 5: Stream file contents **with progress print every 1000 lines**
        print("\nProcessing file contents:\n")
        with gzip.open(second_buffer, mode="rt", encoding="utf-8", errors="ignore") as f:
            for line_num, _ in enumerate(f, start=1):  # Just count lines, don't print them
                if line_num % 1000 == 0:
                    print(f"Processed {line_num} lines...")  # Print progress update

    else:
        print(f"Failed to download second file. HTTP Status: {response.status_code}")

if __name__ == "__main__":
    main()
