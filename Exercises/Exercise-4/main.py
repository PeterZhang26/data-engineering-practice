import os
from pathlib import Path
import json
import csv

def flatten_json(data, parent_key='', sep='_'):
    """
    Recursively flattens nested JSON objects.
    """
    flat_dict = {} # Start with an empty dictionary
    for key, value in data.items(): # Loop through all key-value pairs in dictionary
        new_key = f"{parent_key}{sep}{key}" if parent_key else key # Generate new key by combining parent key and current key
        if isinstance(value, dict):  # If value is a nested dictionary, recurse
            flat_dict.update(flatten_json(value, new_key, sep))
        elif isinstance(value, list) and all(isinstance(i, (int, float)) for i in value):  # Handle coordinates as separate columns
            flat_dict[new_key + "_x"] = value[0]
            flat_dict[new_key + "_y"] = value[1]
        else:
            flat_dict[new_key] = value
    return flat_dict

def main():
    # Step 1: Find all the JSON files
    json_files = list(Path("data").rglob("*.json")) # Find all .json files inside the data/ folder
    print(f"Found {len(json_files)} JSON files.")

    # Step 2: Process each JSON file
    for json_file in json_files:
        print(f"Processing {json_file}...")
    
        # Step 3:Read the JSON file
        with open(json_files[0], "r", encoding="utf-8") as f:
            data = json.load(f) # Load JSON data into a Python dictionary

        # Step 4: Ensure JSON data is a list (even if it's a single object)
        if isinstance(data, dict):
            data = [data]  # Convert single dictionary into a list for consistent processing

        # Step 5: Flatten all records in JSON list
        flattened_records = [flatten_json(record) for record in data]

        # Step 6: Collect all field names dynamically
        all_keys = set()
        for record in flattened_records:
            all_keys.update(record.keys())

        # Step 7: Ensure the output directory exists
        csv_filename = json_file.with_suffix(".csv")  # Change .json to .csv
        csv_filename.parent.mkdir(parents=True, exist_ok=True)  # Ensure subdirectories exist

        # Step 8: Write to CSV file
        with open(csv_filename, "w", newline="", encoding="utf-8") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=sorted(all_keys))  # Ensure all headers are included
            writer.writeheader()
            writer.writerows(flattened_records)  # Write all rows

        print(f"Saved CSV: {csv_filename}")

if __name__ == "__main__":
    main()
